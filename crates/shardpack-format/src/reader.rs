use std::{collections::VecDeque, io::Read};

use bytes::Bytes;
use snafu::ResultExt;

use crate::{
    BLOCK_HEADER_SIZE, BlockHeader, ContentFingerprint, Error, RECORD_ENTRY_SIZE, RecordEntry,
    Result, SHARD_HEADER_SIZE, ShardHeader, checksum::crc32c, encryption, error::IoSnafu,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub sample_id: [u8; 16],
    pub flags: u32,
    pub payload: Bytes,
}

#[derive(Clone, Copy, Debug)]
pub struct ReaderLimits {
    pub max_records_per_block: u32,
    pub max_stored_body_bytes: u64,
    pub max_raw_payload_bytes: u64,
}

impl Default for ReaderLimits {
    fn default() -> Self {
        Self {
            max_records_per_block: 1_000_000,
            max_stored_body_bytes: 8 * 1024 * 1024 * 1024,
            max_raw_payload_bytes: 8 * 1024 * 1024 * 1024,
        }
    }
}

pub struct ShardReader<R> {
    input: R,
    pending: VecDeque<Record>,
    fingerprint: ContentFingerprint,
    block_count: u32,
    limits: ReaderLimits,
    decryption_key: Option<crate::EncryptionKey>,
    derived_decryption_key: Option<([u8; 16], crate::EncryptionKey)>,
    expected_encryption: Option<crate::EncryptionAlgorithm>,
    collect_content_fingerprint: bool,
    decompressor: zrip::DecompressContext,
}

impl<R: Read> ShardReader<R> {
    pub fn new(input: R) -> Result<Self> {
        Self::with_limits_and_key(input, ReaderLimits::default(), None)
    }

    pub fn new_encrypted(input: R, decryption_key: crate::EncryptionKey) -> Result<Self> {
        Self::with_limits_and_key(input, ReaderLimits::default(), Some(decryption_key))
    }

    pub fn with_limits(input: R, limits: ReaderLimits) -> Result<Self> {
        Self::with_limits_and_key(input, limits, None)
    }

    pub fn with_limits_and_key(
        mut input: R,
        limits: ReaderLimits,
        decryption_key: Option<crate::EncryptionKey>,
    ) -> Result<Self> {
        let mut header = [0; SHARD_HEADER_SIZE];
        input.read_exact(&mut header).context(IoSnafu)?;
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
            pending: VecDeque::new(),
            fingerprint: ContentFingerprint::default(),
            block_count: 0,
            limits,
            decryption_key,
            derived_decryption_key: None,
            expected_encryption: Some(expected_encryption),
            collect_content_fingerprint: true,
            decompressor: zrip::DecompressContext::new(),
        })
    }

    /// Skip the order-independent record fingerprint on trusted training hot
    /// paths that validate the exact shard BLAKE3 identity separately.
    #[must_use]
    pub const fn without_content_fingerprint(mut self) -> Self {
        self.collect_content_fingerprint = false;
        self
    }

    pub fn next_record(&mut self) -> Result<Option<Record>> {
        if let Some(record) = self.pending.pop_front() {
            return Ok(Some(record));
        }
        if !self.read_block()? {
            return Ok(None);
        }
        Ok(self.pending.pop_front())
    }

    #[must_use]
    pub const fn content_fingerprint(&self) -> ContentFingerprint {
        self.fingerprint
    }

    #[must_use]
    pub const fn block_count(&self) -> u32 {
        self.block_count
    }

    fn read_block(&mut self) -> Result<bool> {
        let mut raw_header = [0; BLOCK_HEADER_SIZE];
        let read = self.input.read(&mut raw_header[..1]).context(IoSnafu)?;
        if read == 0 {
            return Ok(false);
        }
        self.input
            .read_exact(&mut raw_header[1..])
            .map_err(|source| {
                if source.kind() == std::io::ErrorKind::UnexpectedEof {
                    Error::TruncatedBlock
                } else {
                    Error::Io { source }
                }
            })?;
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
        check_limit(
            "record_count",
            u64::from(header.record_count),
            u64::from(self.limits.max_records_per_block),
        )?;
        check_limit(
            "stored_body_bytes",
            header.stored_body_bytes,
            self.limits.max_stored_body_bytes,
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
        let stored_body_size =
            usize::try_from(header.stored_body_bytes).map_err(|_| Error::InvalidBlockHeader {
                reason: "stored block body too large for platform",
            })?;
        let raw_size =
            usize::try_from(header.raw_payload_bytes).map_err(|_| Error::InvalidBlockHeader {
                reason: "raw payload too large for platform",
            })?;
        let mut body = vec![0; stored_body_size];
        self.input
            .read_exact(&mut body)
            .map_err(map_block_read_error)?;
        if crc32c(&body) != header.payload_crc32c {
            return Err(Error::PayloadChecksumMismatch);
        }
        if header.encryption != crate::EncryptionAlgorithm::None {
            if self
                .derived_decryption_key
                .as_ref()
                .is_none_or(|(salt, _)| salt != &header.key_salt)
            {
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
        let body = Bytes::from(body);
        let directory = body.slice(..expected_directory);
        let stored = body.slice(expected_directory..);
        let payload = header
            .codec
            .decompress(stored, raw_size, &mut self.decompressor)?;
        let mut offset = 0_usize;
        let (entry_chunks, remainder) = directory.as_chunks::<RECORD_ENTRY_SIZE>();
        debug_assert!(remainder.is_empty());
        self.pending.reserve(entry_chunks.len());
        for entry_bytes in entry_chunks {
            let entry = RecordEntry::decode(entry_bytes)?;
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
        self.block_count += 1;
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
    use crate::{
        BLOCK_HEADER_SIZE, BlockCodec, BlockHeader, EncryptionKey, Error, SHARD_HEADER_SIZE,
        ShardReader, ShardWriter, WriterOptions, checksum::crc32c,
    };
    use bytes::Bytes;
    use std::io::Cursor;

    #[test]
    fn sequential_round_trip_with_zstd_and_jumbo_record() {
        let options = WriterOptions {
            target_block_bytes: 8,
            codec: BlockCodec::Zstd,
            zstd_level: 1,
        };
        let mut writer = ShardWriter::new(Vec::new(), options).unwrap();
        let expected = vec![([1; 16], 3, b"abc".to_vec()), ([2; 16], 4, vec![9; 20])];
        for (id, flags, payload) in &expected {
            writer.add_record(*id, *flags, payload).unwrap();
        }
        let (bytes, summary) = writer.finish().unwrap();
        assert_eq!(summary.shard_id, *blake3::hash(&bytes).as_bytes());
        assert_eq!(summary.block_count, 2);
        let mut reader = ShardReader::new(Cursor::new(bytes)).unwrap();
        for (id, flags, payload) in expected {
            let record = reader.next_record().unwrap().unwrap();
            assert_eq!(
                (record.sample_id, record.flags, record.payload),
                (id, flags, Bytes::from(payload))
            );
        }
        assert!(reader.next_record().unwrap().is_none());
        assert_eq!(reader.content_fingerprint(), summary.content_fingerprint);
    }

    fn one_record_shard() -> Vec<u8> {
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([7; 16], 0, b"payload").unwrap();
        writer.finish().unwrap().0
    }

    #[test]
    fn rejects_corrupt_shard_header() {
        let mut bytes = one_record_shard();
        bytes[18] ^= 1;
        assert!(matches!(
            ShardReader::new(Cursor::new(bytes)),
            Err(Error::HeaderChecksumMismatch)
        ));
    }

    #[test]
    fn rejects_corrupt_block_body() {
        let mut bytes = one_record_shard();
        *bytes.last_mut().unwrap() ^= 1;
        let mut reader = ShardReader::new(Cursor::new(bytes)).unwrap();
        assert!(matches!(
            reader.next_record(),
            Err(Error::PayloadChecksumMismatch)
        ));
    }

    #[test]
    fn rejects_truncated_block() {
        let mut bytes = one_record_shard();
        bytes.pop();
        let mut reader = ShardReader::new(Cursor::new(bytes)).unwrap();
        assert!(matches!(reader.next_record(), Err(Error::TruncatedBlock)));
    }

    #[test]
    fn rejects_block_count_overflow_before_buffering_records() {
        let bytes = one_record_shard();
        let mut reader = ShardReader::new(Cursor::new(bytes)).unwrap();
        reader.block_count = u32::MAX;
        assert!(matches!(
            reader.next_record(),
            Err(Error::ResourceLimit {
                field: "block_count",
                ..
            })
        ));
        assert!(reader.pending.is_empty());
    }

    #[test]
    fn encrypted_round_trip_hides_directory_and_payload_and_rejects_wrong_key() {
        let key = EncryptionKey::new([0x31; 32]);
        let wrong_key = EncryptionKey::new([0x32; 32]);
        let mut writer = ShardWriter::new_encrypted(
            Vec::new(),
            WriterOptions {
                target_block_bytes: 4,
                codec: BlockCodec::Zstd,
                zstd_level: 1,
            },
            key.clone(),
        )
        .unwrap();
        writer.add_record([0xab; 16], 7, b"secret-payload").unwrap();
        let (bytes, summary) = writer.finish().unwrap();
        assert_eq!(&bytes[8..10], &crate::FORMAT_VERSION.to_le_bytes());
        let block_header: [u8; BLOCK_HEADER_SIZE] = bytes
            [SHARD_HEADER_SIZE..SHARD_HEADER_SIZE + BLOCK_HEADER_SIZE]
            .try_into()
            .unwrap();
        assert_eq!(
            BlockHeader::decode(&block_header).unwrap().version,
            crate::FORMAT_VERSION
        );
        assert!(!bytes.windows(16).any(|window| window == [0xab; 16]));
        assert!(
            !bytes
                .windows(b"secret-payload".len())
                .any(|window| window == b"secret-payload")
        );

        assert!(matches!(
            ShardReader::new(Cursor::new(bytes.clone())),
            Err(Error::MissingDecryptionKey)
        ));
        let mut inconsistent = bytes.clone();
        let header_end = SHARD_HEADER_SIZE + BLOCK_HEADER_SIZE;
        let raw: [u8; BLOCK_HEADER_SIZE] = inconsistent[SHARD_HEADER_SIZE..header_end]
            .try_into()
            .unwrap();
        let mut header = BlockHeader::decode(&raw).unwrap();
        header.version = 1;
        header.encryption = crate::EncryptionAlgorithm::None;
        header.key_salt = [0; 16];
        header.nonce_counter = 0;
        inconsistent[SHARD_HEADER_SIZE..header_end].copy_from_slice(&header.encode());
        let mut inconsistent_reader =
            ShardReader::new_encrypted(Cursor::new(inconsistent), key.clone()).unwrap();
        assert!(matches!(
            inconsistent_reader.next_record(),
            Err(Error::InvalidBlockHeader {
                reason: "block encryption differs from shard header"
            })
        ));
        let mut wrong = ShardReader::new_encrypted(Cursor::new(bytes.clone()), wrong_key).unwrap();
        assert!(matches!(
            wrong.next_record(),
            Err(Error::AuthenticationFailed)
        ));
        let mut reader = ShardReader::new_encrypted(Cursor::new(bytes), key).unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.sample_id, [0xab; 16]);
        assert_eq!(record.flags, 7);
        assert_eq!(record.payload, "secret-payload");
        assert!(reader.next_record().unwrap().is_none());
        assert_eq!(reader.content_fingerprint(), summary.content_fingerprint);
    }

    fn encrypted_test_shard(
        codec: BlockCodec,
        target_block_bytes: usize,
    ) -> (Vec<u8>, EncryptionKey) {
        let key = EncryptionKey::new([0x71; 32]);
        let mut writer = ShardWriter::new_encrypted(
            Vec::new(),
            WriterOptions {
                target_block_bytes,
                codec,
                zstd_level: 1,
            },
            key.clone(),
        )
        .unwrap();
        writer.add_record([1; 16], 1, b"small").unwrap();
        writer.add_record([2; 16], 2, &[0x55; 40]).unwrap();
        writer.add_record([3; 16], 3, b"tail").unwrap();
        (writer.finish().unwrap().0, key)
    }

    #[test]
    fn encrypted_round_trip_covers_codecs_multiple_blocks_and_jumbo_records() {
        for codec in [BlockCodec::None, BlockCodec::Zstd] {
            let (bytes, key) = encrypted_test_shard(codec, 8);
            let mut reader = ShardReader::new_encrypted(Cursor::new(bytes), key).unwrap();
            let records = std::iter::from_fn(|| reader.next_record().transpose())
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(records.len(), 3);
            assert_eq!(records[0].payload, "small");
            assert_eq!(records[1].payload, Bytes::from_static(&[0x55; 40]));
            assert_eq!(records[2].payload, "tail");
            assert_eq!(reader.block_count(), 3);
        }
    }

    #[test]
    fn encrypted_shard_rejects_every_mid_block_truncation() {
        let (bytes, key) = encrypted_test_shard(BlockCodec::Zstd, 8);
        let mut valid_boundaries = vec![SHARD_HEADER_SIZE];
        let mut offset = SHARD_HEADER_SIZE;
        while offset < bytes.len() {
            let header_end = offset + BLOCK_HEADER_SIZE;
            let raw_header: [u8; BLOCK_HEADER_SIZE] = bytes[offset..header_end].try_into().unwrap();
            let header = BlockHeader::decode(&raw_header).unwrap();
            offset = header_end + header.stored_body_bytes as usize;
            valid_boundaries.push(offset);
        }
        assert_eq!(offset, bytes.len());

        for length in 0..bytes.len() {
            let input = Cursor::new(bytes[..length].to_vec());
            let Ok(mut reader) = ShardReader::new_encrypted(input, key.clone()) else {
                continue;
            };
            let result = std::iter::from_fn(|| reader.next_record().transpose())
                .collect::<Result<Vec<_>, _>>();
            if valid_boundaries.contains(&length) {
                assert!(
                    result.is_ok(),
                    "rejected complete block prefix of {length} bytes"
                );
            } else {
                assert!(
                    result.is_err(),
                    "accepted encrypted shard truncated mid-block to {length} bytes"
                );
            }
        }
    }

    #[test]
    fn encrypted_shard_authenticates_nonce_salt_ciphertext_and_tag() {
        let (bytes, key) = encrypted_test_shard(BlockCodec::None, 1024);
        let block_offset = SHARD_HEADER_SIZE;
        let header_end = block_offset + BLOCK_HEADER_SIZE;
        let mut raw_header: [u8; BLOCK_HEADER_SIZE] =
            bytes[block_offset..header_end].try_into().unwrap();
        let original_header = BlockHeader::decode(&raw_header).unwrap();
        let body_end = header_end + original_header.stored_body_bytes as usize;

        for mutation in ["nonce", "salt", "ciphertext", "tag"] {
            let mut corrupted = bytes.clone();
            let mut header = original_header.clone();
            match mutation {
                "nonce" => header.nonce_counter ^= 1,
                "salt" => header.key_salt[0] ^= 1,
                "ciphertext" => {
                    corrupted[header_end] ^= 1;
                    header.payload_crc32c = crc32c(&corrupted[header_end..body_end]);
                }
                "tag" => {
                    corrupted[body_end - 1] ^= 1;
                    header.payload_crc32c = crc32c(&corrupted[header_end..body_end]);
                }
                _ => unreachable!(),
            }
            raw_header = header.encode();
            corrupted[block_offset..header_end].copy_from_slice(&raw_header);

            let mut reader =
                ShardReader::new_encrypted(Cursor::new(corrupted), key.clone()).unwrap();
            assert!(
                matches!(reader.next_record(), Err(Error::AuthenticationFailed)),
                "{mutation} mutation was not rejected by AEAD"
            );
        }
    }
}
