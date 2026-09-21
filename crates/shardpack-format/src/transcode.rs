//! Streaming-cache helpers that remove physical block encryption without
//! decompressing or changing record payload bytes.

use crate::{
    BLOCK_HEADER_SIZE, BlockHeader, EncryptionAlgorithm, EncryptionKey, Error, Result,
    SHARD_HEADER_SIZE, ShardHeader, checksum::crc32c, encryption,
};

/// Rewrites an encrypted shard header into the canonical plaintext v1 header.
pub fn plaintext_shard_header(raw: &[u8; SHARD_HEADER_SIZE]) -> Result<[u8; SHARD_HEADER_SIZE]> {
    let mut header = ShardHeader::decode(raw)?;
    if header.flags != 1 {
        return Err(Error::InvalidShardHeader {
            reason: "plaintext cache input is not encrypted",
        });
    }
    header.flags = 0;
    Ok(header.encode())
}

/// Authenticates and decrypts one block, preserving its directory and stored
/// (possibly compressed) payload bytes, then emits a valid plaintext header.
pub fn plaintext_block(
    raw_header: &[u8; BLOCK_HEADER_SIZE],
    mut stored_body: Vec<u8>,
    master_key: &EncryptionKey,
) -> Result<([u8; BLOCK_HEADER_SIZE], Vec<u8>)> {
    let mut header = BlockHeader::decode(raw_header)?;
    if header.encryption != EncryptionAlgorithm::Aes256Gcm {
        return Err(Error::InvalidBlockHeader {
            reason: "plaintext cache input block is not encrypted",
        });
    }
    if stored_body.len() as u64 != header.stored_body_bytes {
        return Err(Error::TruncatedBlock);
    }
    if crc32c(&stored_body) != header.payload_crc32c {
        return Err(Error::PayloadChecksumMismatch);
    }
    let key = master_key.derive_for_shard(&header.key_salt);
    encryption::decrypt(
        &key,
        &encryption::block_nonce(header.nonce_counter),
        &raw_header[..56],
        &mut stored_body,
    )?;
    header.encryption = EncryptionAlgorithm::None;
    header.key_salt = [0; 16];
    header.nonce_counter = 0;
    header.stored_body_bytes = stored_body.len() as u64;
    header.payload_crc32c = crc32c(&stored_body);
    Ok((header.encode(), stored_body))
}
