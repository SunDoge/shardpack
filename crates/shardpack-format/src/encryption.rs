use std::sync::Arc;

use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use zeroize::Zeroize;

use crate::{Error, Result};

pub(crate) const AEAD_TAG_BYTES: usize = 16;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum EncryptionAlgorithm {
    #[default]
    None = 0,
    Aes256Gcm = 1,
}

impl TryFrom<u8> for EncryptionAlgorithm {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Aes256Gcm),
            algorithm => Err(Error::UnsupportedEncryption { algorithm }),
        }
    }
}

struct Secret {
    bytes: [u8; 32],
    cipher: LessSafeKey,
}

impl Drop for Secret {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

/// Process-local AES-256 key material. Manifests store only a key ID.
#[derive(Clone)]
pub struct EncryptionKey(Arc<Secret>);

impl EncryptionKey {
    #[must_use]
    pub fn new(bytes: [u8; 32]) -> Self {
        let cipher = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, &bytes).expect("AES-256 key has fixed length"),
        );
        Self(Arc::new(Secret { bytes, cipher }))
    }

    pub(crate) fn bytes(&self) -> &[u8; 32] {
        &self.0.bytes
    }

    /// Non-secret, domain-separated commitment used to bind trusted local
    /// plaintext cache entries to the key that authenticated their source.
    #[must_use]
    pub fn cache_commitment(&self) -> [u8; 32] {
        blake3::derive_key("shardpack plaintext cache key commitment v1", self.bytes())
    }

    fn cipher(&self) -> &LessSafeKey {
        &self.0.cipher
    }

    pub(crate) fn derive_for_shard(&self, salt: &[u8; 16]) -> Self {
        let mut material = [0_u8; 48];
        material[..32].copy_from_slice(self.bytes());
        material[32..].copy_from_slice(salt);
        let derived = blake3::derive_key("shardpack aes-256-gcm shard key v1", &material);
        material.zeroize();
        Self::new(derived)
    }
}

pub(crate) fn block_nonce(counter: u16) -> [u8; 12] {
    let mut nonce = [0_u8; 12];
    nonce[10..].copy_from_slice(&counter.to_be_bytes());
    nonce
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EncryptionKey([REDACTED])")
    }
}

pub(crate) fn encrypt(
    key: &EncryptionKey,
    nonce: &[u8; 12],
    aad: &[u8],
    body: &mut Vec<u8>,
) -> Result<()> {
    let tag = key
        .cipher()
        .seal_in_place_separate_tag(Nonce::assume_unique_for_key(*nonce), Aad::from(aad), body)
        .map_err(|_| Error::AuthenticationFailed)?;
    body.extend_from_slice(tag.as_ref());
    Ok(())
}

pub(crate) fn decrypt(
    key: &EncryptionKey,
    nonce: &[u8; 12],
    aad: &[u8],
    body: &mut Vec<u8>,
) -> Result<()> {
    let tag_offset = body
        .len()
        .checked_sub(AEAD_TAG_BYTES)
        .ok_or(Error::AuthenticationFailed)?;
    let tag: [u8; AEAD_TAG_BYTES] = body[tag_offset..]
        .try_into()
        .expect("tag offset leaves exactly one authentication tag");
    body.truncate(tag_offset);
    key.cipher()
        .open_in_place_separate_tag(
            Nonce::assume_unique_for_key(*nonce),
            Aad::from(aad),
            &tag,
            body,
        )
        .map(|_| ())
        .map_err(|_| Error::AuthenticationFailed)
}
