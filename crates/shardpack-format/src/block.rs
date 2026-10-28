use crate::{BlockCodec, EncryptionAlgorithm, Error, Result, checksum::crc32c};

pub const BLOCK_HEADER_SIZE: usize = 64;
const MAGIC: &[u8; 6] = b"SPKBLK";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockHeader {
    pub version: u16,
    pub flags: u16,
    pub record_count: u32,
    pub directory_bytes: u32,
    pub stored_body_bytes: u64,
    pub raw_payload_bytes: u64,
    pub codec: BlockCodec,
    pub encryption: EncryptionAlgorithm,
    pub key_salt: [u8; 16],
    pub nonce_counter: u32,
    pub payload_crc32c: u32,
}

impl BlockHeader {
    #[must_use]
    pub fn encode(&self) -> [u8; BLOCK_HEADER_SIZE] {
        let mut out = [0; BLOCK_HEADER_SIZE];
        out[..6].copy_from_slice(MAGIC);
        out[6..8].copy_from_slice(&self.version.to_le_bytes());
        out[8..10].copy_from_slice(&self.flags.to_le_bytes());
        out[10..14].copy_from_slice(&self.record_count.to_le_bytes());
        out[14..18].copy_from_slice(&self.directory_bytes.to_le_bytes());
        out[18..26].copy_from_slice(&self.stored_body_bytes.to_le_bytes());
        out[26..34].copy_from_slice(&self.raw_payload_bytes.to_le_bytes());
        out[34] = self.codec as u8;
        out[35] = self.encryption as u8;
        out[36..52].copy_from_slice(&self.key_salt);
        out[52..56].copy_from_slice(&self.nonce_counter.to_le_bytes());
        let header_checksum = crc32c(&out[..56]);
        out[56..60].copy_from_slice(&header_checksum.to_le_bytes());
        out[60..].copy_from_slice(&self.payload_crc32c.to_le_bytes());
        out
    }
    pub fn decode(bytes: &[u8; BLOCK_HEADER_SIZE]) -> Result<Self> {
        if &bytes[..6] != MAGIC {
            return Err(Error::InvalidBlockHeader {
                reason: "bad magic",
            });
        }
        let version = u16::from_le_bytes(bytes[6..8].try_into().expect("fixed slice"));
        if version != crate::FORMAT_VERSION {
            return Err(Error::UnsupportedVersion { version });
        }
        let expected = u32::from_le_bytes(bytes[56..60].try_into().expect("fixed slice"));
        if crc32c(&bytes[..56]) != expected {
            return Err(Error::HeaderChecksumMismatch);
        }
        let encryption = EncryptionAlgorithm::try_from(bytes[35])?;
        let key_salt: [u8; 16] = bytes[36..52].try_into().expect("fixed slice");
        let nonce_counter = u32::from_le_bytes(bytes[52..56].try_into().expect("fixed slice"));
        let flags = u16::from_le_bytes(bytes[8..10].try_into().expect("fixed slice"));
        let record_count = u32::from_le_bytes(bytes[10..14].try_into().expect("fixed slice"));
        if record_count == 0 {
            return Err(Error::InvalidBlockHeader {
                reason: "block contains no records",
            });
        }
        if flags & !1 != 0 || (flags == 1 && record_count != 1) {
            return Err(Error::InvalidBlockHeader {
                reason: "invalid jumbo block flags",
            });
        }
        if encryption == EncryptionAlgorithm::None && (key_salt != [0; 16] || nonce_counter != 0) {
            return Err(Error::InvalidBlockHeader {
                reason: "unencrypted block has key or nonce material",
            });
        }
        Ok(Self {
            version,
            flags,
            record_count,
            directory_bytes: u32::from_le_bytes(bytes[14..18].try_into().expect("fixed slice")),
            stored_body_bytes: u64::from_le_bytes(bytes[18..26].try_into().expect("fixed slice")),
            raw_payload_bytes: u64::from_le_bytes(bytes[26..34].try_into().expect("fixed slice")),
            codec: BlockCodec::try_from(bytes[34])?,
            encryption,
            key_salt,
            nonce_counter,
            payload_crc32c: u32::from_le_bytes(bytes[60..].try_into().expect("fixed slice")),
        })
    }

    /// Validates the canonical salt and nonce sequence for an encrypted shard.
    /// The u32 counter covers the full u32 block-index space; the block-count
    /// checks in readers and writers reject indices at `u32::MAX` before any
    /// wraparound can occur.
    pub fn validate_encrypted_sequence(
        &self,
        block_index: u32,
        shard_salt: Option<&[u8; 16]>,
    ) -> Result<()> {
        if self.encryption == EncryptionAlgorithm::None {
            return Err(Error::InvalidBlockHeader {
                reason: "encryption sequence validation requires an encrypted block",
            });
        }
        if self.nonce_counter != block_index {
            return Err(Error::InvalidBlockHeader {
                reason: "encrypted block nonce is not its sequential block index",
            });
        }
        if shard_salt.is_some_and(|salt| salt != &self.key_salt) {
            return Err(Error::InvalidBlockHeader {
                reason: "encrypted block salt differs within shard",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{BlockCodec, EncryptionAlgorithm, Error};

    use super::{BLOCK_HEADER_SIZE, BlockHeader};

    fn header() -> BlockHeader {
        BlockHeader {
            version: 1,
            flags: 0,
            record_count: 1,
            directory_bytes: crate::RECORD_ENTRY_SIZE as u32,
            stored_body_bytes: 1,
            raw_payload_bytes: 1,
            codec: BlockCodec::None,
            encryption: EncryptionAlgorithm::None,
            key_salt: [0; 16],
            nonce_counter: 0,
            payload_crc32c: 0,
        }
    }

    #[test]
    fn rejects_empty_blocks_and_invalid_jumbo_flags() {
        let mut empty = header();
        empty.record_count = 0;
        assert!(matches!(
            BlockHeader::decode(&empty.encode()),
            Err(Error::InvalidBlockHeader {
                reason: "block contains no records"
            })
        ));

        let mut invalid_jumbo = header();
        invalid_jumbo.flags = 1;
        invalid_jumbo.record_count = 2;
        assert!(matches!(
            BlockHeader::decode(&invalid_jumbo.encode()),
            Err(Error::InvalidBlockHeader {
                reason: "invalid jumbo block flags"
            })
        ));
    }

    #[test]
    fn rejects_a_separate_encrypted_format_version() {
        let mut encrypted = header();
        encrypted.version = 2;
        encrypted.encryption = EncryptionAlgorithm::Aes256Gcm;
        assert!(matches!(
            BlockHeader::decode(&encrypted.encode()),
            Err(Error::UnsupportedVersion { version: 2 })
        ));
    }

    #[test]
    fn nonce_counter_round_trips_the_full_u32_range_without_allocation() {
        for counter in [
            0_u32,
            1,
            u32::from(u16::MAX),
            u32::from(u16::MAX) + 1,
            u32::MAX,
        ] {
            let mut block = header();
            block.encryption = EncryptionAlgorithm::Aes256Gcm;
            block.key_salt = [7; 16];
            block.nonce_counter = counter;
            let encoded = block.encode();
            // Widened little-endian field at offsets 52..56.
            assert_eq!(&encoded[52..56], &counter.to_le_bytes());
            assert_eq!(BlockHeader::decode(&encoded).unwrap(), block);
            // The counter spans the entire u32 block-index space.
            block
                .validate_encrypted_sequence(counter, Some(&[7; 16]))
                .unwrap();
        }
    }

    #[test]
    fn rejects_pre_widening_sixty_four_bit_magic_layout() {
        // An old-layout header keeps its trailing magic NULs where the widened
        // nonce now lives: the version field reads zero and decoding fails
        // closed instead of silently misreading the old encryption framing.
        let mut legacy = [0_u8; BLOCK_HEADER_SIZE];
        legacy[..8].copy_from_slice(b"SPKBLK\0\0");
        assert!(matches!(
            BlockHeader::decode(&legacy),
            Err(Error::UnsupportedVersion { version: 0 })
        ));
    }
}
