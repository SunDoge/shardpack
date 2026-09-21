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
    pub(crate) fn decompress(
        self,
        stored: Bytes,
        raw_size: usize,
        context: &mut zrip::DecompressContext,
    ) -> Result<Bytes> {
        match self {
            Self::None if stored.len() == raw_size => Ok(stored),
            Self::None => Err(Error::InvalidBlockHeader {
                reason: "raw and stored sizes differ for codec none",
            }),
            Self::Zstd => {
                let mut output = Vec::with_capacity(raw_size);
                let written = context
                    .decompress_into_with_limit(&stored, &mut output, raw_size)
                    .map_err(|source| Error::Decompression { source })?;
                if written != raw_size {
                    return Err(Error::InvalidBlockHeader {
                        reason: "decompressed payload size differs from raw payload size",
                    });
                }
                Ok(Bytes::from(output))
            }
        }
    }
}
