use std::collections::VecDeque;

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    BLOCK_HEADER_SIZE, BlockHeader, ContentFingerprint, Error, RECORD_ENTRY_SIZE, ReaderLimits,
    Record, RecordEntry, Result, SHARD_HEADER_SIZE, ShardHeader,
    checksum::{crc32c, crc32c_slices},
    encryption,
};

pub struct AsyncShardReader<R> {
    input: R,
    pending: VecDeque<Record>,
    fingerprint: ContentFingerprint,
    block_count: u32,
    records_yielded: u64,
    limits: ReaderLimits,
    byte_offset: u64,
    current_block_start_offset: u64,
    current_block_record_index: u32,
    decryption_key: Option<crate::EncryptionKey>,
    collect_content_fingerprint: bool,
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
        ShardHeader::decode(&header)?;
        Ok(Self {
            input,
            pending: VecDeque::new(),
            fingerprint: ContentFingerprint::default(),
            block_count: 0,
            records_yielded: 0,
            limits,
            byte_offset: SHARD_HEADER_SIZE as u64,
            current_block_start_offset: SHARD_HEADER_SIZE as u64,
            current_block_record_index: 0,
            decryption_key,
            collect_content_fingerprint: true,
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

    pub fn from_block_boundary_with_key(
        input: R,
        limits: ReaderLimits,
        block_index: u32,
        block_start_offset: u64,
        decryption_key: Option<crate::EncryptionKey>,
    ) -> Self {
        Self {
            input,
            pending: VecDeque::new(),
            fingerprint: ContentFingerprint::default(),
            block_count: block_index,
            records_yielded: 0,
            limits,
            byte_offset: block_start_offset,
            current_block_start_offset: block_start_offset,
            current_block_record_index: 0,
            decryption_key,
            collect_content_fingerprint: true,
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
        if let Some(record) = self.pending.pop_front() {
            self.records_yielded += 1;
            self.current_block_record_index += 1;
            return Ok(Some(record));
        }
        if !self.read_block().await? {
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

    #[must_use]
    pub const fn input_ref(&self) -> &R {
        &self.input
    }

    #[must_use]
    pub const fn position(&self) -> AsyncReaderPosition {
        AsyncReaderPosition {
            block_index: self.block_count.saturating_sub(1),
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
        check_limit(
            "record_count",
            u64::from(header.record_count),
            u64::from(self.limits.max_records_per_block),
        )?;
        check_limit(
            "stored_payload_bytes",
            header.stored_payload_bytes,
            self.limits.max_stored_payload_bytes,
        )?;
        check_limit(
            "raw_payload_bytes",
            header.raw_payload_bytes,
            self.limits.max_raw_payload_bytes,
        )?;
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
        let stored_size = usize::try_from(header.stored_payload_bytes).map_err(|_| {
            Error::InvalidBlockHeader {
                reason: "stored payload too large for platform",
            }
        })?;
        let raw_size =
            usize::try_from(header.raw_payload_bytes).map_err(|_| Error::InvalidBlockHeader {
                reason: "raw payload too large for platform",
            })?;
        let (directory, stored, physical_body_bytes) =
            if header.encryption == crate::EncryptionAlgorithm::None {
                let mut directory = vec![0; expected_directory];
                let mut stored = vec![0; stored_size];
                self.input
                    .read_exact(&mut directory)
                    .await
                    .map_err(map_block_read_error)?;
                self.input
                    .read_exact(&mut stored)
                    .await
                    .map_err(map_block_read_error)?;
                if crc32c_slices(&directory, &stored) != header.payload_crc32c {
                    return Err(Error::PayloadChecksumMismatch);
                }
                (
                    bytes::Bytes::from(directory),
                    bytes::Bytes::from(stored),
                    header.directory_bytes as u64 + header.stored_payload_bytes,
                )
            } else {
                let mut body = vec![0; stored_size];
                self.input
                    .read_exact(&mut body)
                    .await
                    .map_err(map_block_read_error)?;
                if crc32c(&body) != header.payload_crc32c {
                    return Err(Error::PayloadChecksumMismatch);
                }
                let key = self
                    .decryption_key
                    .as_ref()
                    .ok_or(Error::MissingDecryptionKey)?;
                let key = key.derive_for_shard(&header.key_salt);
                encryption::decrypt(
                    &key,
                    &encryption::block_nonce(header.nonce_counter),
                    &raw_header[..56],
                    &mut body,
                )?;
                if body.len() < expected_directory {
                    return Err(Error::InvalidDirectory {
                        reason: "encrypted body is shorter than directory",
                    });
                }
                let body = bytes::Bytes::from(body);
                (
                    body.slice(..expected_directory),
                    body.slice(expected_directory..),
                    header.stored_payload_bytes,
                )
            };
        self.byte_offset = self
            .byte_offset
            .checked_add(BLOCK_HEADER_SIZE as u64)
            .and_then(|value| value.checked_add(physical_body_bytes))
            .ok_or(Error::InvalidBlockHeader {
                reason: "block offset overflow",
            })?;
        let payload = header.codec.decompress(stored, raw_size)?;
        let mut offset = 0_usize;
        let (entries, remainder) = directory.as_chunks::<RECORD_ENTRY_SIZE>();
        debug_assert!(remainder.is_empty());
        for raw_entry in entries {
            let entry = RecordEntry::decode(raw_entry)?;
            let end =
                offset
                    .checked_add(entry.payload_size as usize)
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
                self.fingerprint.insert(&entry.sample_id, &bytes);
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
        self.block_count = self
            .block_count
            .checked_add(1)
            .expect("block count overflow");
        Ok(true)
    }
}

fn check_limit(field: &'static str, actual: u64, limit: u64) -> Result<()> {
    if actual > limit {
        Err(Error::ResourceLimit {
            field,
            actual,
            limit,
        })
    } else {
        Ok(())
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

    use crate::{AsyncShardReader, BlockCodec, EncryptionKey, ShardWriter, WriterOptions};

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
        let mut reader = AsyncShardReader::new_encrypted(std::io::Cursor::new(bytes), key)
            .await
            .unwrap();
        let record = reader.next_record().await.unwrap().unwrap();
        assert_eq!(record.sample_id, [5; 16]);
        assert_eq!(record.payload, "encrypted");
        assert!(reader.next_record().await.unwrap().is_none());
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
