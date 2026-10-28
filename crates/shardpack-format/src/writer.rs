use std::io::Write;

use snafu::ResultExt;

use crate::{
    BlockCodec, BlockHeader, ContentFingerprint, EncryptionAlgorithm, FORMAT_VERSION,
    RECORD_ENTRY_SIZE, RecordEntry, Result, ShardHeader,
    checksum::{crc32c, crc32c_slices},
    encryption,
    error::IoSnafu,
};

#[derive(Clone, Debug)]
pub struct WriterOptions {
    pub target_block_bytes: usize,
    pub codec: BlockCodec,
    pub zstd_level: i32,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            target_block_bytes: 16 * 1024 * 1024,
            codec: BlockCodec::None,
            zstd_level: 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardSummary {
    pub shard_id: [u8; 32],
    pub byte_size: u64,
    pub record_count: u64,
    pub block_count: u32,
    pub content_fingerprint: ContentFingerprint,
}

pub struct ShardWriter<W> {
    output: W,
    hasher: blake3::Hasher,
    byte_size: u64,
    options: WriterOptions,
    entries: Vec<RecordEntry>,
    directory: Vec<u8>,
    payload: Vec<u8>,
    encrypted_body: Vec<u8>,
    fingerprint: ContentFingerprint,
    block_count: u32,
    encryption_key: Option<crate::EncryptionKey>,
    encryption_salt: [u8; 16],
    compressor: Option<zrip::CompressContext>,
}

impl<W: Write> ShardWriter<W> {
    pub fn new(output: W, options: WriterOptions) -> Result<Self> {
        Self::new_inner(output, options, None)
    }

    pub fn new_encrypted(
        output: W,
        options: WriterOptions,
        encryption_key: crate::EncryptionKey,
    ) -> Result<Self> {
        Self::new_inner(output, options, Some(encryption_key))
    }

    fn new_inner(
        output: W,
        options: WriterOptions,
        encryption_key: Option<crate::EncryptionKey>,
    ) -> Result<Self> {
        let mut encryption_salt = [0_u8; 16];
        if encryption_key.is_some() {
            getrandom::fill(&mut encryption_salt)
                .map_err(|source| crate::Error::Random { source })?;
        }
        let encryption_key = encryption_key.map(|key| key.derive_for_shard(&encryption_salt));
        let compressor = if options.codec == BlockCodec::Zstd {
            let mut compressor = zrip::CompressContext::new(options.zstd_level)
                .map_err(|source| crate::Error::Compression { source })?;
            // The enclosing block CRC32C (and optional AEAD) already provides
            // integrity, so a second full-input zstd checksum is redundant.
            compressor.set_content_checksum(false);
            Some(compressor)
        } else {
            None
        };
        let mut writer = Self {
            output,
            hasher: blake3::Hasher::new(),
            byte_size: 0,
            options,
            entries: Vec::new(),
            directory: Vec::new(),
            payload: Vec::new(),
            encrypted_body: Vec::new(),
            fingerprint: ContentFingerprint::default(),
            block_count: 0,
            encryption_key,
            encryption_salt,
            compressor,
        };
        let encrypted = writer.encryption_key.is_some();
        writer.write_exact(
            &ShardHeader {
                version: FORMAT_VERSION,
                flags: u16::from(encrypted),
            }
            .encode(),
        )?;
        Ok(writer)
    }

    pub fn add_record(&mut self, sample_id: [u8; 16], flags: u32, payload: &[u8]) -> Result<()> {
        let payload_size =
            u64::try_from(payload.len()).map_err(|_| crate::Error::InvalidRecord {
                reason: "payload size cannot be represented as u64",
            })?;
        let mut next_block_bytes =
            buffered_block_bytes(self.entries.len(), self.payload.len(), payload.len())?;
        if !self.entries.is_empty()
            && (next_block_bytes > self.options.target_block_bytes
                || self.entries.len() == u32::MAX as usize)
        {
            self.flush_block()?;
            next_block_bytes = buffered_block_bytes(0, 0, payload.len())?;
        }
        self.entries.push(RecordEntry {
            sample_id,
            payload_size,
            flags,
        });
        self.payload.extend_from_slice(payload);
        self.fingerprint.insert(&sample_id, flags, payload);
        if next_block_bytes >= self.options.target_block_bytes {
            self.flush_block()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<(W, ShardSummary)> {
        self.flush_block()?;
        self.output.flush().context(IoSnafu)?;
        let summary = ShardSummary {
            shard_id: *self.hasher.finalize().as_bytes(),
            byte_size: self.byte_size,
            record_count: self.fingerprint.record_count,
            block_count: self.block_count,
            content_fingerprint: self.fingerprint,
        };
        Ok((self.output, summary))
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        if self.block_count == u32::MAX {
            return Err(crate::Error::InvalidRecord {
                reason: "shard contains too many blocks",
            });
        }
        self.directory.clear();
        let directory_bytes = self.entries.len().checked_mul(RECORD_ENTRY_SIZE).ok_or(
            crate::Error::InvalidDirectory {
                reason: "directory size overflow",
            },
        )?;
        u32::try_from(directory_bytes).map_err(|_| crate::Error::InvalidDirectory {
            reason: "directory exceeds u32 size",
        })?;
        self.directory.try_reserve(directory_bytes).map_err(|_| {
            crate::Error::InvalidDirectory {
                reason: "cannot allocate record directory",
            }
        })?;
        for entry in &self.entries {
            self.directory.extend_from_slice(&entry.encode());
        }
        let compressed = match self.options.codec {
            BlockCodec::None => None,
            BlockCodec::Zstd => Some(
                self.compressor
                    .as_mut()
                    .expect("zstd writer has a compression context")
                    .compress(&self.payload)
                    .map_err(|source| crate::Error::Compression { source })?
                    .into_owned(),
            ),
        };
        let stored = compressed.as_deref().unwrap_or(&self.payload);
        let encrypted = self.encryption_key.is_some();
        // The u32 counter covers the entire u32 block-index space; the
        // block-count ceiling check above rejects u32::MAX before any
        // wraparound, so the counter never repeats within a shard.
        let nonce_counter = if encrypted { self.block_count } else { 0 };
        let body_bytes = self
            .directory
            .len()
            .checked_add(stored.len())
            .and_then(|size| {
                size.checked_add(if encrypted {
                    encryption::AEAD_TAG_BYTES
                } else {
                    0
                })
            })
            .ok_or(crate::Error::InvalidRecord {
                reason: "stored block body size overflow",
            })?;
        let mut header = BlockHeader {
            version: FORMAT_VERSION,
            flags: u16::from(
                self.entries.len() == 1
                    && self
                        .directory
                        .len()
                        .checked_add(self.payload.len())
                        .is_none_or(|bytes| bytes > self.options.target_block_bytes),
            ),
            record_count: u32::try_from(self.entries.len()).map_err(|_| {
                crate::Error::InvalidRecord {
                    reason: "too many records in block",
                }
            })?,
            directory_bytes: u32::try_from(self.directory.len()).map_err(|_| {
                crate::Error::InvalidDirectory {
                    reason: "directory exceeds u32 size",
                }
            })?,
            stored_body_bytes: body_bytes as u64,
            raw_payload_bytes: self.payload.len() as u64,
            codec: self.options.codec,
            encryption: if encrypted {
                EncryptionAlgorithm::Aes256Gcm
            } else {
                EncryptionAlgorithm::None
            },
            key_salt: if encrypted {
                self.encryption_salt
            } else {
                [0; 16]
            },
            nonce_counter,
            payload_crc32c: 0,
        };
        if let Some(key) = &self.encryption_key {
            self.encrypted_body.clear();
            self.encrypted_body.reserve(body_bytes);
            self.encrypted_body.extend_from_slice(&self.directory);
            self.encrypted_body.extend_from_slice(stored);
            let encoded = header.encode();
            encryption::encrypt(
                key,
                &encryption::block_nonce(nonce_counter),
                &encoded[..56],
                &mut self.encrypted_body,
            )?;
            header.payload_crc32c = crc32c(&self.encrypted_body);
            write_exact_parts(
                &mut self.output,
                &mut self.hasher,
                &mut self.byte_size,
                &header.encode(),
            )?;
            write_exact_parts(
                &mut self.output,
                &mut self.hasher,
                &mut self.byte_size,
                &self.encrypted_body,
            )?;
            // Successful AEAD sealing has overwritten the plaintext. Keep the
            // ciphertext allocation for the next block; ciphertext does not
            // require secret-memory erasure.
            self.encrypted_body.clear();
        } else {
            header.payload_crc32c = crc32c_slices(&self.directory, stored);
            write_exact_parts(
                &mut self.output,
                &mut self.hasher,
                &mut self.byte_size,
                &header.encode(),
            )?;
            write_exact_parts(
                &mut self.output,
                &mut self.hasher,
                &mut self.byte_size,
                &self.directory,
            )?;
            write_exact_parts(
                &mut self.output,
                &mut self.hasher,
                &mut self.byte_size,
                stored,
            )?;
        }
        self.entries.clear();
        self.payload.clear();
        self.block_count += 1;
        Ok(())
    }

    fn write_exact(&mut self, bytes: &[u8]) -> Result<()> {
        write_exact_parts(
            &mut self.output,
            &mut self.hasher,
            &mut self.byte_size,
            bytes,
        )
    }
}

fn buffered_block_bytes(
    buffered_records: usize,
    buffered_payload_bytes: usize,
    next_payload_bytes: usize,
) -> Result<usize> {
    buffered_records
        .checked_add(1)
        .and_then(|records| records.checked_mul(RECORD_ENTRY_SIZE))
        .and_then(|directory| directory.checked_add(buffered_payload_bytes))
        .and_then(|bytes| bytes.checked_add(next_payload_bytes))
        .ok_or(crate::Error::InvalidRecord {
            reason: "block buffer size overflow",
        })
}

fn write_exact_parts<W: Write>(
    output: &mut W,
    hasher: &mut blake3::Hasher,
    byte_size: &mut u64,
    bytes: &[u8],
) -> Result<()> {
    let next_byte_size =
        byte_size
            .checked_add(bytes.len() as u64)
            .ok_or(crate::Error::InvalidRecord {
                reason: "shard byte size exceeds u64",
            })?;
    output.write_all(bytes).context(IoSnafu)?;
    hasher.update(bytes);
    *byte_size = next_byte_size;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Error as IoError, Write};

    use crate::{BlockCodec, EncryptionKey, Error, SHARD_HEADER_SIZE, ShardReader, encryption};

    use super::{ShardWriter, WriterOptions, write_exact_parts};

    #[derive(Default)]
    struct ShortWriter {
        bytes: Vec<u8>,
        max_write: usize,
    }

    impl Write for ShortWriter {
        fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
            let count = input.len().min(self.max_write.max(1));
            self.bytes.extend_from_slice(&input[..count]);
            Ok(count)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct FailAfter {
        bytes: Vec<u8>,
        remaining: usize,
        fail_flush: bool,
    }

    impl Write for FailAfter {
        fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Err(IoError::other("injected write failure"));
            }
            let count = input.len().min(self.remaining).min(3);
            self.bytes.extend_from_slice(&input[..count]);
            self.remaining -= count;
            Ok(count)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            if self.fail_flush {
                Err(IoError::other("injected flush failure"))
            } else {
                Ok(())
            }
        }
    }

    fn options() -> WriterOptions {
        WriterOptions {
            target_block_bytes: 4,
            codec: BlockCodec::None,
            zstd_level: 1,
        }
    }

    fn write_test_shard<W: Write>(output: W) -> crate::Result<(W, super::ShardSummary)> {
        let mut writer = ShardWriter::new(output, options())?;
        writer.add_record([1; 16], 7, b"abc")?;
        writer.add_record([2; 16], 9, b"defgh")?;
        writer.finish()
    }

    #[test]
    fn accepts_short_writes_and_hashes_exact_emitted_bytes() {
        let output = ShortWriter {
            bytes: Vec::new(),
            max_write: 1,
        };
        let (output, summary) = write_test_shard(output).unwrap();

        assert_eq!(summary.shard_id, *blake3::hash(&output.bytes).as_bytes());
        assert_eq!(summary.byte_size, output.bytes.len() as u64);
        let mut reader = ShardReader::new(std::io::Cursor::new(output.bytes)).unwrap();
        assert_eq!(reader.next_record().unwrap().unwrap().payload, "abc");
        assert_eq!(reader.next_record().unwrap().unwrap().payload, "defgh");
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn reuses_encrypted_block_scratch_capacity() {
        let mut writer =
            ShardWriter::new_encrypted(Vec::new(), options(), EncryptionKey::new([3; 32])).unwrap();
        writer.add_record([1; 16], 0, b"abcd").unwrap();
        let allocation = writer.encrypted_body.as_ptr();
        let capacity = writer.encrypted_body.capacity();
        assert!(capacity >= 24 + 4 + encryption::AEAD_TAG_BYTES);

        writer.add_record([2; 16], 0, b"efgh").unwrap();
        assert_eq!(writer.encrypted_body.as_ptr(), allocation);
        assert_eq!(writer.encrypted_body.capacity(), capacity);
        writer.finish().unwrap();
    }

    #[test]
    fn directory_bytes_count_toward_the_block_target() {
        let mut writer = ShardWriter::new(
            Vec::new(),
            WriterOptions {
                target_block_bytes: 2 * crate::RECORD_ENTRY_SIZE,
                codec: BlockCodec::None,
                zstd_level: 1,
            },
        )
        .unwrap();
        for value in 0..5_u8 {
            writer.add_record([value; 16], 0, b"").unwrap();
        }
        let (bytes, summary) = writer.finish().unwrap();
        assert_eq!(summary.record_count, 5);
        assert_eq!(summary.block_count, 3);

        let mut reader = ShardReader::new(std::io::Cursor::new(bytes)).unwrap();
        let mut records = 0;
        while reader.next_record().unwrap().is_some() {
            records += 1;
        }
        assert_eq!(records, 5);
        assert_eq!(reader.block_count(), 3);
    }

    #[test]
    fn rejects_block_count_overflow_before_writing_the_block() {
        let mut writer = ShardWriter::new(Vec::new(), options()).unwrap();
        writer.block_count = u32::MAX;
        let bytes_before = writer.output.len();
        assert!(matches!(
            writer.add_record([1; 16], 0, b"abcd"),
            Err(Error::InvalidRecord {
                reason: "shard contains too many blocks"
            })
        ));
        assert_eq!(writer.output.len(), bytes_before);
    }

    #[test]
    fn nonce_counter_spans_the_full_u32_block_index_space() {
        // The pre-widening u16 ceiling no longer binds encrypted shards.
        let beyond_old_nonce_space = u32::from(u16::MAX) + 1;
        let mut encrypted =
            ShardWriter::new_encrypted(Vec::new(), options(), EncryptionKey::new([9; 32])).unwrap();
        encrypted.block_count = beyond_old_nonce_space;
        encrypted.add_record([1; 16], 0, b"abcd").unwrap();
        assert_eq!(
            encrypted.finish().unwrap().1.block_count,
            beyond_old_nonce_space + 1
        );

        // Both layouts reject the u32 ceiling before writing, preventing
        // counter wraparound.
        for encrypted in [false, true] {
            let mut writer = if encrypted {
                ShardWriter::new_encrypted(Vec::new(), options(), EncryptionKey::new([3; 32]))
                    .unwrap()
            } else {
                ShardWriter::new(Vec::new(), options()).unwrap()
            };
            writer.block_count = u32::MAX;
            let bytes_before = writer.output.len();
            assert!(matches!(
                writer.add_record([1; 16], 0, b"abcd"),
                Err(Error::InvalidRecord {
                    reason: "shard contains too many blocks"
                })
            ));
            assert_eq!(writer.output.len(), bytes_before);
        }
    }

    #[tokio::test]
    async fn encrypted_nonce_sequence_crosses_the_u16_boundary() {
        let key = EncryptionKey::new([0x45; 32]);
        let mut writer = ShardWriter::new_encrypted(
            Vec::new(),
            WriterOptions {
                target_block_bytes: 1,
                codec: BlockCodec::None,
                zstd_level: 1,
            },
            key.clone(),
        )
        .unwrap();
        // Seed the counter at the old u16 ceiling: blocks 65535, 65536 and
        // 65537 exercise counters on both sides of the widened space.
        writer.block_count = u32::from(u16::MAX);
        for value in 1..=3_u8 {
            writer.add_record([value; 16], 0, &[value; 1]).unwrap();
        }
        let (bytes, summary) = writer.finish().unwrap();
        assert_eq!(summary.block_count, u32::from(u16::MAX) + 3);

        // A sequential reader starting at block zero must still reject the
        // counter mismatch instead of silently accepting shifted nonces.
        let mut sequential =
            ShardReader::new_encrypted(std::io::Cursor::new(&bytes), key.clone()).unwrap();
        assert!(matches!(
            sequential.next_record(),
            Err(Error::InvalidBlockHeader {
                reason: "encrypted block nonce is not its sequential block index"
            })
        ));

        // Boundary resume at the seeded index validates every crossing block.
        let mut resumed = crate::AsyncShardReader::from_block_boundary_with_key(
            std::io::Cursor::new(&bytes[crate::SHARD_HEADER_SIZE..]),
            crate::ReaderLimits::default(),
            u32::from(u16::MAX),
            crate::SHARD_HEADER_SIZE as u64,
            Some(key),
        );
        for value in 1..=3_u8 {
            let record = resumed.next_record().await.unwrap().unwrap();
            assert_eq!(record.sample_id, [value; 16]);
            assert_eq!(record.payload.as_ref(), &[value; 1]);
        }
        assert!(resumed.next_record().await.unwrap().is_none());
        assert_eq!(resumed.block_count(), u32::from(u16::MAX) + 3);
    }

    #[test]
    fn rejects_byte_count_overflow_before_mutating_output_or_hash() {
        let mut output = Vec::new();
        let mut hasher = blake3::Hasher::new();
        let initial_digest = hasher.finalize();
        let mut byte_size = u64::MAX;
        assert!(matches!(
            write_exact_parts(&mut output, &mut hasher, &mut byte_size, b"x"),
            Err(Error::InvalidRecord {
                reason: "shard byte size exceeds u64"
            })
        ));
        assert!(output.is_empty());
        assert_eq!(hasher.finalize(), initial_digest);
        assert_eq!(byte_size, u64::MAX);
    }

    #[test]
    fn propagates_an_io_error_at_every_output_byte() {
        let complete_len = write_test_shard(Vec::new()).unwrap().0.len();

        for fail_after in 0..complete_len {
            let output = FailAfter {
                bytes: Vec::new(),
                remaining: fail_after,
                fail_flush: false,
            };
            let result = write_test_shard(output);
            assert!(
                matches!(result, Err(Error::Io { .. })),
                "write unexpectedly succeeded with failure after byte {fail_after}"
            );
        }

        assert!(complete_len > SHARD_HEADER_SIZE);
    }

    #[test]
    fn propagates_flush_failure() {
        let output = FailAfter {
            bytes: Vec::new(),
            remaining: usize::MAX,
            fail_flush: true,
        };
        assert!(matches!(write_test_shard(output), Err(Error::Io { .. })));
    }
}
