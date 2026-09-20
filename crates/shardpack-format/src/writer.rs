use std::io::Write;

use snafu::ResultExt;
use zeroize::Zeroize;

use crate::{
    BlockCodec, BlockHeader, ContentFingerprint, ENCRYPTED_FORMAT_VERSION, EncryptionAlgorithm,
    RecordEntry, Result, ShardHeader,
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
            fingerprint: ContentFingerprint::default(),
            block_count: 0,
            encryption_key,
            encryption_salt,
            compressor,
        };
        let encrypted = writer.encryption_key.is_some();
        writer.write_exact(
            &ShardHeader {
                version: if encrypted {
                    ENCRYPTED_FORMAT_VERSION
                } else {
                    crate::FORMAT_VERSION
                },
                flags: u16::from(encrypted),
                ..ShardHeader::default()
            }
            .encode(),
        )?;
        Ok(writer)
    }

    pub fn add_record(&mut self, sample_id: [u8; 16], flags: u32, payload: &[u8]) -> Result<()> {
        let payload_size =
            u32::try_from(payload.len()).map_err(|_| crate::Error::InvalidRecord {
                reason: "payload exceeds u32 size",
            })?;
        if !self.entries.is_empty()
            && self.payload.len().saturating_add(payload.len()) > self.options.target_block_bytes
        {
            self.flush_block()?;
        }
        self.entries.push(RecordEntry {
            sample_id,
            payload_size,
            flags,
        });
        self.payload.extend_from_slice(payload);
        self.fingerprint.insert(&sample_id, payload);
        if self.payload.len() >= self.options.target_block_bytes {
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
        self.directory.clear();
        self.directory.reserve(self.entries.len() * 24);
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
        let nonce_counter =
            u16::try_from(self.block_count).map_err(|_| crate::Error::InvalidRecord {
                reason: "encrypted shard exceeds 65536 blocks",
            })?;
        let body_bytes = if encrypted {
            self.directory
                .len()
                .checked_add(stored.len())
                .and_then(|size| size.checked_add(encryption::AEAD_TAG_BYTES))
                .ok_or(crate::Error::InvalidRecord {
                    reason: "encrypted block size overflow",
                })?
        } else {
            stored.len()
        };
        let mut header = BlockHeader {
            version: if encrypted { 2 } else { 1 },
            flags: u16::from(
                self.entries.len() == 1 && self.payload.len() > self.options.target_block_bytes,
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
            stored_payload_bytes: body_bytes as u64,
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
            nonce_counter: if encrypted { nonce_counter } else { 0 },
            payload_crc32c: 0,
        };
        if let Some(key) = &self.encryption_key {
            let mut body = Vec::with_capacity(body_bytes);
            body.extend_from_slice(&self.directory);
            body.extend_from_slice(stored);
            let encoded = header.encode();
            encryption::encrypt(
                key,
                &encryption::block_nonce(nonce_counter),
                &encoded[..56],
                &mut body,
            )?;
            header.payload_crc32c = crc32c(&body);
            self.write_exact(&header.encode())?;
            self.write_exact(&body)?;
            body.zeroize();
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
        self.block_count = self
            .block_count
            .checked_add(1)
            .expect("block count overflow");
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

fn write_exact_parts<W: Write>(
    output: &mut W,
    hasher: &mut blake3::Hasher,
    byte_size: &mut u64,
    bytes: &[u8],
) -> Result<()> {
    output.write_all(bytes).context(IoSnafu)?;
    hasher.update(bytes);
    *byte_size = byte_size
        .checked_add(bytes.len() as u64)
        .expect("shard size overflow");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Error as IoError, Write};

    use crate::{BlockCodec, Error, SHARD_HEADER_SIZE, ShardReader};

    use super::{ShardWriter, WriterOptions};

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
