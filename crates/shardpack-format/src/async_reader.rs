use std::collections::VecDeque;

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    BLOCK_HEADER_SIZE, BlockHeader, ContentFingerprint, Error, RECORD_ENTRY_SIZE, ReaderLimits,
    Record, RecordEntry, Result, SHARD_HEADER_SIZE, ShardHeader, checksum::crc32c, encryption,
};

pub struct AsyncShardReader<R> {
    input: R,
    failed: bool,
    pending: VecDeque<Record>,
    fingerprint: ContentFingerprint,
    block_count: u32,
    current_block_index: u32,
    records_yielded: u64,
    limits: ReaderLimits,
    byte_offset: u64,
    current_block_start_offset: u64,
    current_block_record_index: u32,
    decryption_key: Option<crate::EncryptionKey>,
    derived_decryption_key: Option<([u8; 16], crate::EncryptionKey)>,
    expected_encryption: Option<crate::EncryptionAlgorithm>,
    collect_content_fingerprint: bool,
    decompressor: zrip::DecompressContext,
    stored_buffer: crate::reusable_buffer::ReusableBuffer,
    decoded_buffer: crate::reusable_buffer::ReusableBuffer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AsyncReaderPosition {
    pub block_index: u32,
    pub block_start_offset: u64,
    pub record_index: u32,
    pub next_block_offset: u64,
}

impl<R: AsyncRead + Unpin> AsyncShardReader<R> {
    pub async fn new(input: R) -> Result<Self> {
        Self::with_limits_and_key(input, ReaderLimits::default(), None).await
    }

    pub async fn new_encrypted(input: R, decryption_key: crate::EncryptionKey) -> Result<Self> {
        Self::with_limits_and_key(input, ReaderLimits::default(), Some(decryption_key)).await
    }

    pub async fn with_limits(input: R, limits: ReaderLimits) -> Result<Self> {
        Self::with_limits_and_key(input, limits, None).await
    }

    pub async fn with_limits_and_key(
        mut input: R,
        limits: ReaderLimits,
        decryption_key: Option<crate::EncryptionKey>,
    ) -> Result<Self> {
        let mut header = [0; SHARD_HEADER_SIZE];
        input
            .read_exact(&mut header)
            .await
            .map_err(|source| Error::Io { source })?;
        let header = ShardHeader::decode(&header)?;
        let expected_encryption = if header.flags == 1 {
            if decryption_key.is_none() {
                return Err(Error::MissingDecryptionKey);
            }
            crate::EncryptionAlgorithm::Aes256Gcm
        } else {
            crate::EncryptionAlgorithm::None
        };
        Ok(Self {
            input,
            failed: false,
            pending: VecDeque::new(),
            fingerprint: ContentFingerprint::default(),
            block_count: 0,
            current_block_index: 0,
            records_yielded: 0,
            limits,
            byte_offset: SHARD_HEADER_SIZE as u64,
            current_block_start_offset: SHARD_HEADER_SIZE as u64,
            current_block_record_index: 0,
            decryption_key,
            derived_decryption_key: None,
            expected_encryption: Some(expected_encryption),
            collect_content_fingerprint: true,
            decompressor: zrip::DecompressContext::new(),
            stored_buffer: crate::reusable_buffer::ReusableBuffer::default(),
            decoded_buffer: crate::reusable_buffer::ReusableBuffer::default(),
        })
    }

    pub fn from_block_boundary(
        input: R,
        limits: ReaderLimits,
        block_index: u32,
        block_start_offset: u64,
    ) -> Self {
        Self::from_block_boundary_with_key(input, limits, block_index, block_start_offset, None)
    }

    /// Resumes a sequential read at a physical block boundary. The presence of
    /// `decryption_key` selects the expected encryption mode because the shard
    /// header is not re-read at a boundary: callers MUST pass `None` for
    /// plaintext (including plaintext cache) objects, or the first block is
    /// rejected as an encryption mismatch. Unlike
    /// [`ShardReader::with_limits_and_key`], a key supplied for a plaintext
    /// shard is not silently ignored.
    pub fn from_block_boundary_with_key(
        input: R,
        limits: ReaderLimits,
        block_index: u32,
        block_start_offset: u64,
        decryption_key: Option<crate::EncryptionKey>,
    ) -> Self {
        let expected_encryption = Some(if decryption_key.is_some() {
            crate::EncryptionAlgorithm::Aes256Gcm
        } else {
            crate::EncryptionAlgorithm::None
        });
        Self {
            input,
            failed: false,
            pending: VecDeque::new(),
            fingerprint: ContentFingerprint::default(),
            block_count: block_index,
            current_block_index: block_index,
            records_yielded: 0,
            limits,
            byte_offset: block_start_offset,
            current_block_start_offset: block_start_offset,
            current_block_record_index: 0,
            decryption_key,
            derived_decryption_key: None,
            expected_encryption,
            collect_content_fingerprint: true,
            decompressor: zrip::DecompressContext::new(),
            stored_buffer: crate::reusable_buffer::ReusableBuffer::default(),
            decoded_buffer: crate::reusable_buffer::ReusableBuffer::default(),
        }
    }

    /// Skip the order-independent record fingerprint on trusted training hot
    /// paths that validate the exact shard BLAKE3 identity separately.
    #[must_use]
    pub const fn without_content_fingerprint(mut self) -> Self {
        self.collect_content_fingerprint = false;
        self
    }

    pub async fn next_record(&mut self) -> Result<Option<Record>> {
        if self.failed {
            return Err(Error::ReaderFailed);
        }
        if let Some(record) = self.pending.pop_front() {
            self.records_yielded += 1;
            self.current_block_record_index += 1;
            return Ok(Some(record));
        }
        let fingerprint = self.fingerprint;
        let loaded = match self.read_block().await {
            Ok(loaded) => loaded,
            Err(error) => {
                self.failed = true;
                self.pending.clear();
                self.stored_buffer.clear();
                self.decoded_buffer.clear();
                self.fingerprint = fingerprint;
                return Err(error);
            }
        };
        if !loaded {
            self.stored_buffer.clear();
            self.decoded_buffer.clear();
            return Ok(None);
        }
        let record = self.pending.pop_front();
        if record.is_some() {
            self.records_yielded += 1;
            self.current_block_record_index += 1;
        }
        Ok(record)
    }

    #[must_use]
    pub const fn content_fingerprint(&self) -> ContentFingerprint {
        self.fingerprint
    }

    #[must_use]
    pub const fn block_count(&self) -> u32 {
        self.block_count
    }

    #[must_use]
    pub const fn records_yielded(&self) -> u64 {
        self.records_yielded
    }

    /// Records already decoded from the current block. Returning one of these
    /// does not poll the underlying object stream.
    #[must_use]
    pub fn buffered_records(&self) -> usize {
        self.pending.len()
    }

    #[must_use]
    pub const fn input_ref(&self) -> &R {
        &self.input
    }

    #[must_use]
    pub const fn position(&self) -> AsyncReaderPosition {
        AsyncReaderPosition {
            block_index: self.current_block_index,
            block_start_offset: self.current_block_start_offset,
            record_index: self.current_block_record_index,
            next_block_offset: self.byte_offset,
        }
    }

    pub async fn skip_records(&mut self, count: u32) -> Result<()> {
        for _ in 0..count {
            if self.next_record().await?.is_none() {
                return Err(Error::InvalidRecord {
                    reason: "checkpoint record index exceeds block",
                });
            }
        }
        Ok(())
    }

    async fn read_block(&mut self) -> Result<bool> {
        self.current_block_start_offset = self.byte_offset;
        self.current_block_record_index = 0;
        self.current_block_index = self.block_count;
        let mut raw_header = [0; BLOCK_HEADER_SIZE];
        let read = self
            .input
            .read(&mut raw_header[..1])
            .await
            .map_err(|source| Error::Io { source })?;
        if read == 0 {
            return Ok(false);
        }
        self.input
            .read_exact(&mut raw_header[1..])
            .await
            .map_err(map_block_read_error)?;
        let header = BlockHeader::decode(&raw_header)?;
        if self.block_count == u32::MAX {
            return Err(Error::ResourceLimit {
                field: "block_count",
                actual: u64::from(u32::MAX) + 1,
                limit: u64::from(u32::MAX),
            });
        }
        if self
            .expected_encryption
            .is_some_and(|expected| header.encryption != expected)
        {
            return Err(Error::InvalidBlockHeader {
                reason: "block encryption differs from shard header",
            });
        }
        if header.encryption != crate::EncryptionAlgorithm::None && self.decryption_key.is_none() {
            return Err(Error::MissingDecryptionKey);
        }
        self.limits.validate_header(&header)?;
        let expected_directory = usize::try_from(header.record_count)
            .expect("u32 fits usize")
            .checked_mul(RECORD_ENTRY_SIZE)
            .ok_or(Error::InvalidDirectory {
                reason: "directory size overflow",
            })?;
        if header.directory_bytes as usize != expected_directory {
            return Err(Error::InvalidDirectory {
                reason: "directory size does not match record count",
            });
        }
        let stored_body_size =
            usize::try_from(header.stored_body_bytes).map_err(|_| Error::InvalidBlockHeader {
                reason: "stored block body too large for platform",
            })?;
        let raw_size =
            usize::try_from(header.raw_payload_bytes).map_err(|_| Error::InvalidBlockHeader {
                reason: "raw payload too large for platform",
            })?;
        let mut body = self.stored_buffer.take(stored_body_size);
        body.resize(stored_body_size, 0);
        self.input
            .read_exact(&mut body)
            .await
            .map_err(map_block_read_error)?;
        if crc32c(&body) != header.payload_crc32c {
            return Err(Error::PayloadChecksumMismatch);
        }
        if header.encryption != crate::EncryptionAlgorithm::None {
            header.validate_encrypted_sequence(
                self.block_count,
                self.derived_decryption_key.as_ref().map(|(salt, _)| salt),
            )?;
            if self.derived_decryption_key.is_none() {
                let key = self
                    .decryption_key
                    .as_ref()
                    .ok_or(Error::MissingDecryptionKey)?
                    .derive_for_shard(&header.key_salt);
                self.derived_decryption_key = Some((header.key_salt, key));
            }
            let key = &self
                .derived_decryption_key
                .as_ref()
                .expect("derived key was initialized")
                .1;
            encryption::decrypt(
                key,
                &encryption::block_nonce(header.nonce_counter),
                &raw_header[..56],
                &mut body,
            )?;
        }
        if body.len() < expected_directory {
            return Err(Error::InvalidDirectory {
                reason: "stored body is shorter than directory",
            });
        }
        let body = self.stored_buffer.retain(body);
        let directory = body.slice(..expected_directory);
        let stored = body.slice(expected_directory..);
        self.byte_offset = self
            .byte_offset
            .checked_add(BLOCK_HEADER_SIZE as u64)
            .and_then(|value| value.checked_add(header.stored_body_bytes))
            .ok_or(Error::InvalidBlockHeader {
                reason: "block offset overflow",
            })?;
        let payload = header.codec.decompress(
            stored,
            raw_size,
            &mut self.decompressor,
            &mut self.decoded_buffer,
        )?;
        let mut offset = 0_usize;
        let (entries, remainder) = directory.as_chunks::<RECORD_ENTRY_SIZE>();
        debug_assert!(remainder.is_empty());
        self.pending.reserve(entries.len());
        for raw_entry in entries {
            let entry = RecordEntry::decode(raw_entry)?;
            let payload_size =
                usize::try_from(entry.payload_size).map_err(|_| Error::InvalidDirectory {
                    reason: "record size exceeds addressable memory",
                })?;
            let end = offset
                .checked_add(payload_size)
                .ok_or(Error::InvalidDirectory {
                    reason: "record offset overflow",
                })?;
            if end > payload.len() {
                return Err(Error::InvalidDirectory {
                    reason: "record exceeds raw payload",
                });
            }
            let bytes = payload.slice(offset..end);
            if self.collect_content_fingerprint {
                self.fingerprint
                    .insert(&entry.sample_id, entry.flags, &bytes);
            }
            self.pending.push_back(Record {
                sample_id: entry.sample_id,
                flags: entry.flags,
                payload: bytes,
            });
            offset = end;
        }
        if offset != payload.len() {
            return Err(Error::InvalidDirectory {
                reason: "unreferenced payload bytes",
            });
        }
        self.block_count += 1;
        Ok(true)
    }
}

fn map_block_read_error(source: std::io::Error) -> Error {
    if source.kind() == std::io::ErrorKind::UnexpectedEof {
        Error::TruncatedBlock
    } else {
        Error::Io { source }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use tokio::io::{AsyncRead, ReadBuf};

    use crate::{
        AsyncShardReader, BLOCK_HEADER_SIZE, BlockCodec, BlockHeader, EncryptionAlgorithm,
        EncryptionKey, Error, ReaderLimits, SHARD_HEADER_SIZE, ShardWriter, WriterOptions,
    };

    struct ChunkedReader<R> {
        inner: R,
        max_chunk: usize,
    }

    impl<R: AsyncRead + Unpin> AsyncRead for ChunkedReader<R> {
        fn poll_read(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            output: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            let limit = output.remaining().min(this.max_chunk);
            let poll = {
                let mut limited = ReadBuf::new(&mut output.initialize_unfilled()[..limit]);
                match Pin::new(&mut this.inner).poll_read(context, &mut limited) {
                    Poll::Ready(Ok(())) => Poll::Ready(Ok(limited.filled().len())),
                    Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                    Poll::Pending => Poll::Pending,
                }
            };
            match poll {
                Poll::Ready(Ok(filled)) => {
                    output.advance(filled);
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    #[tokio::test]
    async fn reads_stream_without_seek() {
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([4; 16], 8, b"record").unwrap();
        let bytes = writer.finish().unwrap().0;
        let mut reader = AsyncShardReader::new(std::io::Cursor::new(bytes))
            .await
            .unwrap();
        let record = reader.next_record().await.unwrap().unwrap();
        assert_eq!(record.sample_id, [4; 16]);
        assert_eq!(record.payload, "record");
        assert!(reader.next_record().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn decrypts_stream_without_seek() {
        let key = EncryptionKey::new([0x51; 32]);
        let mut writer =
            ShardWriter::new_encrypted(Vec::new(), WriterOptions::default(), key.clone()).unwrap();
        writer.add_record([5; 16], 9, b"encrypted").unwrap();
        let bytes = writer.finish().unwrap().0;
        assert!(matches!(
            AsyncShardReader::new(std::io::Cursor::new(bytes.clone())).await,
            Err(Error::MissingDecryptionKey)
        ));
        let mut reader = AsyncShardReader::new_encrypted(std::io::Cursor::new(bytes), key)
            .await
            .unwrap();
        let record = reader.next_record().await.unwrap().unwrap();
        assert_eq!(record.sample_id, [5; 16]);
        assert_eq!(record.payload, "encrypted");
        assert!(reader.next_record().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn block_boundary_resume_enforces_the_expected_encryption_mode() {
        let key = EncryptionKey::new([0x71; 32]);
        let mut writer =
            ShardWriter::new_encrypted(Vec::new(), WriterOptions::default(), key.clone()).unwrap();
        writer.add_record([7; 16], 0, b"record").unwrap();
        let mut bytes = writer.finish().unwrap().0;
        let block_end = SHARD_HEADER_SIZE + BLOCK_HEADER_SIZE;
        let raw_header: [u8; BLOCK_HEADER_SIZE] =
            bytes[SHARD_HEADER_SIZE..block_end].try_into().unwrap();
        let mut header = BlockHeader::decode(&raw_header).unwrap();
        header.version = 1;
        header.encryption = EncryptionAlgorithm::None;
        header.key_salt = [0; 16];
        header.nonce_counter = 0;
        bytes[SHARD_HEADER_SIZE..block_end].copy_from_slice(&header.encode());

        let mut reader = AsyncShardReader::from_block_boundary_with_key(
            std::io::Cursor::new(bytes[SHARD_HEADER_SIZE..].to_vec()),
            ReaderLimits::default(),
            0,
            SHARD_HEADER_SIZE as u64,
            Some(key),
        );
        assert!(matches!(
            reader.next_record().await,
            Err(Error::InvalidBlockHeader {
                reason: "block encryption differs from shard header"
            })
        ));
    }

    #[tokio::test]
    async fn encrypted_zstd_stream_is_independent_of_underlying_read_chunk_size() {
        let key = EncryptionKey::new([0x61; 32]);
        let mut writer = ShardWriter::new_encrypted(
            Vec::new(),
            WriterOptions {
                target_block_bytes: 7,
                codec: BlockCodec::Zstd,
                zstd_level: 1,
            },
            key.clone(),
        )
        .unwrap();
        for value in 0..6_u8 {
            writer
                .add_record([value; 16], u32::from(value), &[value; 5])
                .unwrap();
        }
        let bytes = writer.finish().unwrap().0;

        for max_chunk in 1..=70 {
            let input = ChunkedReader {
                inner: std::io::Cursor::new(bytes.clone()),
                max_chunk,
            };
            let mut reader = AsyncShardReader::new_encrypted(input, key.clone())
                .await
                .unwrap();
            for value in 0..6_u8 {
                let record = reader.next_record().await.unwrap().unwrap();
                assert_eq!(record.sample_id, [value; 16]);
                assert_eq!(record.flags, u32::from(value));
                assert_eq!(record.payload.as_ref(), &[value; 5]);
            }
            assert!(reader.next_record().await.unwrap().is_none());
        }
    }
}
