use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use shardpack_format::{AsyncShardReader, ContentFingerprint, EncryptionKey, ShardReader};

// Independent framing and RustCrypto encryption, decoded using AWS-LC.
fn reference_vector() -> Vec<u8> {
    let mut shard = [0_u8; 64];
    shard[..8].copy_from_slice(b"SPKSHRD\0");
    shard[8] = 1;
    shard[10] = 1;
    shard[12] = 64;
    let crc = crc32c::crc32c(&shard[..60]);
    shard[60..].copy_from_slice(&crc.to_le_bytes());
    let mut block = [0_u8; 64];
    block[..8].copy_from_slice(b"SPKBLK\0\0");
    block[8] = 1;
    block[12] = 1;
    block[16] = 28;
    block[20] = 49; // directory + five payload bytes + tag
    block[28] = 5;
    block[37] = 1;
    block[38..54].copy_from_slice(&[0x22; 16]);
    let mut material = vec![0x11; 32];
    material.extend_from_slice(&[0x22; 16]);
    let key = blake3::derive_key("shardpack aes-256-gcm shard key v1", &material);
    let mut body = vec![0x33; 16];
    body.extend_from_slice(&5_u64.to_le_bytes());
    body.extend_from_slice(&0x12345678_u32.to_le_bytes());
    body.extend_from_slice(b"alpha");
    let cipher = Aes256Gcm::new_from_slice(&key).unwrap();
    let encrypted = cipher
        .encrypt(
            &Nonce::from([0; 12]),
            Payload {
                msg: &body,
                aad: &block[..56],
            },
        )
        .unwrap();
    let crc = crc32c::crc32c(&block[..56]);
    block[56..60].copy_from_slice(&crc.to_le_bytes());
    block[60..].copy_from_slice(&crc32c::crc32c(&encrypted).to_le_bytes());
    [shard.as_slice(), block.as_slice(), encrypted.as_slice()].concat()
}

#[tokio::test]
async fn independent_crypto_and_fingerprint_vector() {
    let bytes = reference_vector();
    assert_eq!(
        hex::encode(&bytes),
        include_str!("data/v1_aes256gcm.hex").trim()
    );
    let key = EncryptionKey::new([0x11; 32]);
    let mut reader = ShardReader::new_encrypted(bytes.as_slice(), key.clone()).unwrap();
    let mut asynchronous = AsyncShardReader::new_encrypted(bytes.as_slice(), key)
        .await
        .unwrap();
    let record = reader.next_record().unwrap().unwrap();
    assert_eq!(record, asynchronous.next_record().await.unwrap().unwrap());
    assert_eq!(record.sample_id, [0x33; 16]);
    assert_eq!(record.flags, 0x12345678);
    assert_eq!(&record.payload[..], b"alpha");
    assert!(reader.next_record().unwrap().is_none());
    assert!(asynchronous.next_record().await.unwrap().is_none());
    let mut fingerprint = ContentFingerprint::default();
    fingerprint.insert(&[0x33; 16], 0x12345678, b"alpha");
    assert_eq!(
        hex::encode(fingerprint.accumulator_a),
        "6e33d2196d4e064ae74a3874eac66b89db705aa7d1b5eb98adf6ceba4742cc3b"
    );
    assert_eq!(
        hex::encode(fingerprint.accumulator_b),
        "750a7a2985f6565c03377876e7fab7322af662b1ff91750c3dfbf84f11ace14c"
    );
    assert_eq!(reader.content_fingerprint(), fingerprint);
}
