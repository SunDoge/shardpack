use std::io::Cursor;

use shardpack_format::{
    AsyncShardReader, BlockCodec, EncryptionKey, ShardReader, ShardWriter, WriterOptions,
};

fn fixture(codec: BlockCodec, encrypted: bool) -> (Vec<u8>, Option<EncryptionKey>) {
    let options = WriterOptions {
        target_block_bytes: 1024,
        codec,
        zstd_level: 1,
    };
    let key = encrypted.then(|| EncryptionKey::new([7; 32]));
    let mut writer = match &key {
        Some(key) => ShardWriter::new_encrypted(Vec::new(), options, key.clone()).unwrap(),
        None => ShardWriter::new(Vec::new(), options).unwrap(),
    };
    for n in 0..6_u8 {
        writer
            .add_record([n; 16], u32::from(n), &vec![n; 1024])
            .unwrap();
    }
    (writer.finish().unwrap().0, key)
}

#[test]
fn sync_retained_record_survives_other_blocks_and_reader_drop() {
    for codec in [BlockCodec::None, BlockCodec::Zstd] {
        for encrypted in [false, true] {
            let (bytes, key) = fixture(codec, encrypted);
            let mut reader =
                ShardReader::with_limits_and_key(Cursor::new(bytes), Default::default(), key)
                    .unwrap();
            let first = reader.next_record().unwrap().unwrap();
            let mut previous = None;
            for n in 1..6_u8 {
                let record = reader.next_record().unwrap().unwrap();
                assert_eq!(record.payload.as_ref(), &[n; 1024]);
                assert_eq!(record.flags, u32::from(n));
                assert_ne!(record.payload.as_ptr(), first.payload.as_ptr());
                if let Some(pointer) = previous {
                    assert_eq!(
                        record.payload.as_ptr(),
                        pointer,
                        "released block should be reused"
                    );
                }
                previous = Some(record.payload.as_ptr());
            }
            assert!(reader.next_record().unwrap().is_none());
            drop(reader);
            assert_eq!(first.payload.as_ref(), &[0; 1024]);
        }
    }
}

#[tokio::test]
async fn async_retained_record_survives_other_blocks_and_reader_drop() {
    for codec in [BlockCodec::None, BlockCodec::Zstd] {
        for encrypted in [false, true] {
            let (bytes, key) = fixture(codec, encrypted);
            let mut reader =
                AsyncShardReader::with_limits_and_key(Cursor::new(bytes), Default::default(), key)
                    .await
                    .unwrap();
            let first = reader.next_record().await.unwrap().unwrap();
            let mut previous = None;
            for n in 1..6_u8 {
                let record = reader.next_record().await.unwrap().unwrap();
                assert_eq!(record.payload.as_ref(), &[n; 1024]);
                assert_ne!(record.payload.as_ptr(), first.payload.as_ptr());
                if let Some(pointer) = previous {
                    assert_eq!(record.payload.as_ptr(), pointer);
                }
                previous = Some(record.payload.as_ptr());
            }
            assert!(reader.next_record().await.unwrap().is_none());
            drop(reader);
            assert_eq!(first.payload.as_ref(), &[0; 1024]);
        }
    }
}
