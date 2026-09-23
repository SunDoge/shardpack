use crate::{Error, FORMAT_VERSION, Result, checksum::crc32c};

pub const SHARD_HEADER_SIZE: usize = 64;
const MAGIC: &[u8; 8] = b"SPKSHRD\0";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardHeader {
    pub version: u16,
    pub flags: u16,
}

impl Default for ShardHeader {
    fn default() -> Self {
        Self {
            version: FORMAT_VERSION,
            flags: 0,
        }
    }
}

impl ShardHeader {
    #[must_use]
    pub fn encode(&self) -> [u8; SHARD_HEADER_SIZE] {
        let mut out = [0; SHARD_HEADER_SIZE];
        out[..8].copy_from_slice(MAGIC);
        out[8..10].copy_from_slice(&self.version.to_le_bytes());
        out[10..12].copy_from_slice(&self.flags.to_le_bytes());
        out[12..16].copy_from_slice(&(SHARD_HEADER_SIZE as u32).to_le_bytes());
        let header_checksum = crc32c(&out[..60]);
        out[60..].copy_from_slice(&header_checksum.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8; SHARD_HEADER_SIZE]) -> Result<Self> {
        if &bytes[..8] != MAGIC {
            return Err(Error::InvalidShardHeader {
                reason: "bad magic",
            });
        }
        let version = u16::from_le_bytes(bytes[8..10].try_into().expect("fixed slice"));
        if version != FORMAT_VERSION {
            return Err(Error::UnsupportedVersion { version });
        }
        if u32::from_le_bytes(bytes[12..16].try_into().expect("fixed slice")) as usize
            != SHARD_HEADER_SIZE
        {
            return Err(Error::InvalidShardHeader {
                reason: "unsupported header size",
            });
        }
        let expected = u32::from_le_bytes(bytes[60..].try_into().expect("fixed slice"));
        if crc32c(&bytes[..60]) != expected {
            return Err(Error::HeaderChecksumMismatch);
        }
        let flags = u16::from_le_bytes(bytes[10..12].try_into().expect("fixed slice"));
        if flags & !1 != 0 {
            return Err(Error::InvalidShardHeader {
                reason: "unsupported shard flags",
            });
        }
        Ok(Self { version, flags })
    }
}

#[cfg(test)]
mod tests {
    use crate::{Error, FORMAT_VERSION};

    use super::ShardHeader;

    #[test]
    fn one_version_covers_plaintext_and_encrypted_flags() {
        for flags in [0, 1] {
            let header = ShardHeader {
                version: FORMAT_VERSION,
                flags,
            };
            assert_eq!(ShardHeader::decode(&header.encode()).unwrap(), header);
        }

        let unsupported = ShardHeader {
            version: 2,
            ..Default::default()
        };
        assert!(matches!(
            ShardHeader::decode(&unsupported.encode()),
            Err(Error::UnsupportedVersion { version: 2 })
        ));
    }
}
