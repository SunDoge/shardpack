use crate::{Error, Result};

pub const RECORD_ENTRY_SIZE: usize = 28;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordEntry {
    pub sample_id: [u8; 16],
    pub payload_size: u64,
    pub flags: u32,
}

impl RecordEntry {
    #[must_use]
    pub fn encode(&self) -> [u8; RECORD_ENTRY_SIZE] {
        let mut out = [0; RECORD_ENTRY_SIZE];
        out[..16].copy_from_slice(&self.sample_id);
        out[16..24].copy_from_slice(&self.payload_size.to_le_bytes());
        out[24..].copy_from_slice(&self.flags.to_le_bytes());
        out
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != RECORD_ENTRY_SIZE {
            return Err(Error::InvalidDirectory {
                reason: "bad entry size",
            });
        }
        Ok(Self {
            sample_id: bytes[..16].try_into().expect("fixed slice"),
            payload_size: u64::from_le_bytes(bytes[16..24].try_into().expect("fixed slice")),
            flags: u32::from_le_bytes(bytes[24..].try_into().expect("fixed slice")),
        })
    }
}
