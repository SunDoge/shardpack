use bytes::Bytes;

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockCodec {
    #[default]
    None = 0,
    Zstd = 1,
}

impl TryFrom<u8> for BlockCodec {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Zstd),
            codec => Err(Error::UnsupportedCodec { codec }),
        }
    }
}

impl BlockCodec {
    pub(crate) fn decompress(self, stored: Bytes, raw_size: usize) -> Result<Bytes> {
        match self {
            Self::None if stored.len() == raw_size => Ok(stored),
            Self::None => Err(Error::InvalidBlockHeader {
                reason: "raw and stored sizes differ for codec none",
            }),
            // zrip emits standard zstd frames and wins on the transfer-focused
            // encode path. The mature C decoder remains materially faster on
            // highly compressible blocks, which dominate some training sets.
            Self::Zstd => zstd::bulk::decompress(&stored, raw_size)
                .map(Bytes::from)
                .map_err(|source| Error::Decompression { source }),
        }
    }
}
