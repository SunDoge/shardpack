use crate::{BlockCodec, EncryptionAlgorithm, Error, Result, checksum::crc32c};

pub const BLOCK_HEADER_SIZE: usize = 64;
const MAGIC: &[u8; 8] = b"SPKBLK\0\0";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockHeader {
    pub version: u16,
    pub flags: u16,
    pub record_count: u32,
    pub directory_bytes: u32,
    pub stored_payload_bytes: u64,
    pub raw_payload_bytes: u64,
    pub codec: BlockCodec,
    pub encryption: EncryptionAlgorithm,
    pub key_salt: [u8; 16],
    pub nonce_counter: u16,
    pub payload_crc32c: u32,
}

impl BlockHeader {
    #[must_use]
    pub fn encode(&self) -> [u8; BLOCK_HEADER_SIZE] {
        let mut out = [0; BLOCK_HEADER_SIZE];
        out[..8].copy_from_slice(MAGIC);
        out[8..10].copy_from_slice(&self.version.to_le_bytes());
        out[10..12].copy_from_slice(&self.flags.to_le_bytes());
        out[12..16].copy_from_slice(&self.record_count.to_le_bytes());
        out[16..20].copy_from_slice(&self.directory_bytes.to_le_bytes());
        out[20..28].copy_from_slice(&self.stored_payload_bytes.to_le_bytes());
        out[28..36].copy_from_slice(&self.raw_payload_bytes.to_le_bytes());
        out[36] = self.codec as u8;
        out[37] = self.encryption as u8;
        out[38..54].copy_from_slice(&self.key_salt);
        out[54..56].copy_from_slice(&self.nonce_counter.to_le_bytes());
        let header_checksum = crc32c(&out[..56]);
        out[56..60].copy_from_slice(&header_checksum.to_le_bytes());
        out[60..].copy_from_slice(&self.payload_crc32c.to_le_bytes());
        out
    }
    pub fn decode(bytes: &[u8; BLOCK_HEADER_SIZE]) -> Result<Self> {
        if &bytes[..8] != MAGIC {
            return Err(Error::InvalidBlockHeader {
                reason: "bad magic",
            });
        }
        let version = u16::from_le_bytes(bytes[8..10].try_into().expect("fixed slice"));
        if version != 1 && version != 2 {
            return Err(Error::UnsupportedVersion { version });
        }
        let expected = u32::from_le_bytes(bytes[56..60].try_into().expect("fixed slice"));
        if crc32c(&bytes[..56]) != expected {
            return Err(Error::HeaderChecksumMismatch);
        }
        let encryption = EncryptionAlgorithm::try_from(bytes[37])?;
        let key_salt: [u8; 16] = bytes[38..54].try_into().expect("fixed slice");
        let nonce_counter = u16::from_le_bytes(bytes[54..56].try_into().expect("fixed slice"));
        let flags = u16::from_le_bytes(bytes[10..12].try_into().expect("fixed slice"));
        let record_count = u32::from_le_bytes(bytes[12..16].try_into().expect("fixed slice"));
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
        if (version == 1 && encryption != EncryptionAlgorithm::None)
            || (version == 2 && encryption != EncryptionAlgorithm::Aes256Gcm)
            || (encryption == EncryptionAlgorithm::None
                && (key_salt != [0; 16] || nonce_counter != 0))
        {
            return Err(Error::InvalidBlockHeader {
                reason: "version, encryption, or nonce combination is invalid",
            });
        }
        Ok(Self {
            version,
            flags,
            record_count,
            directory_bytes: u32::from_le_bytes(bytes[16..20].try_into().expect("fixed slice")),
            stored_payload_bytes: u64::from_le_bytes(
                bytes[20..28].try_into().expect("fixed slice"),
            ),
            raw_payload_bytes: u64::from_le_bytes(bytes[28..36].try_into().expect("fixed slice")),
            codec: BlockCodec::try_from(bytes[36])?,
            encryption,
            key_salt,
            nonce_counter,
            payload_crc32c: u32::from_le_bytes(bytes[60..].try_into().expect("fixed slice")),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{BlockCodec, EncryptionAlgorithm, Error};

    use super::BlockHeader;

    fn header() -> BlockHeader {
        BlockHeader {
            version: 1,
            flags: 0,
            record_count: 1,
            directory_bytes: 24,
            stored_payload_bytes: 1,
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
}
