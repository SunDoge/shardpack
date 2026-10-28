use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use shardpack_format::{AsyncShardReader, ContentFingerprint, EncryptionKey, ShardReader};

// Independent framing and RustCrypto encryption, decoded using AWS-LC.
fn reference_vector(counter: u32, nonce: [u8; 12]) -> Vec<u8> {
    let mut shard = [0_u8; 64];
    shard[..8].copy_from_slice(b"SPKSHRD\0");
    shard[8] = 1;
    shard[10] = 1;
    shard[12] = 64;
    let crc = crc32c::crc32c(&shard[..60]);
    shard[60..].copy_from_slice(&crc.to_le_bytes());
    let mut block = [0_u8; 64];
    block[..6].copy_from_slice(b"SPKBLK");
    block[6] = 1;
    block[10] = 1;
    block[14] = 28;
    block[18] = 49; // directory + five payload bytes + tag
    block[26] = 5;
    block[35] = 1;
    block[36..52].copy_from_slice(&[0x22; 16]);
    block[52..56].copy_from_slice(&counter.to_le_bytes());
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
            &Nonce::from(nonce),
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
    let bytes = reference_vector(0, [0; 12]);
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

#[tokio::test]
async fn independent_nonzero_nonce_vectors_cover_widening_and_byte_order() {
    // Literal nonces intentionally avoid the production nonce helper and any
    // shared integer-to-nonce conversion. Header counters are LE; nonces are BE.
    let cases = [
        (65_535, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff]),
        (65_536, [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0]),
        (0x0102_0304, [0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4]),
        (
            u32::MAX - 1,
            [0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xfe],
        ),
    ];
    let key = EncryptionKey::new([0x11; 32]);
    for (counter, nonce) in cases {
        let bytes = reference_vector(counter, nonce);
        let mut reader = AsyncShardReader::from_block_boundary_with_key(
            &bytes[64..],
            shardpack_format::ReaderLimits::default(),
            counter,
            64,
            Some(key.clone()),
        );
        let record = reader.next_record().await.unwrap().unwrap();
        assert_eq!(record.sample_id, [0x33; 16]);
        assert_eq!(record.flags, 0x12345678);
        assert_eq!(&record.payload[..], b"alpha");
        assert_eq!(reader.block_count(), counter + 1);
        assert!(reader.next_record().await.unwrap().is_none());

        // Exercise the independent plaintext-cache decryption entry point too.
        let (_, body) = shardpack_format::plaintext_block(
            bytes[64..128].try_into().unwrap(),
            bytes[128..].to_vec(),
            &key,
        )
        .unwrap();
        assert_eq!(&body[..16], &[0x33; 16]);
        assert_eq!(&body[28..], b"alpha");

        // A valid RustCrypto tag made with an incorrect nonce must fail AWS-LC
        // authentication even though the header and body CRCs are valid.
        let mut wrong_nonce = nonce;
        wrong_nonce[0] = 1;
        let invalid = reference_vector(counter, wrong_nonce);
        assert!(matches!(
            shardpack_format::plaintext_block(
                invalid[64..128].try_into().unwrap(),
                invalid[128..].to_vec(),
                &key,
            ),
            Err(shardpack_format::Error::AuthenticationFailed)
        ));
    }
}
