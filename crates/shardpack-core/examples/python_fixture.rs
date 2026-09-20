use bytes::Bytes;
use shardpack_core::{
    EncodedRecord, MaterializeEncryption, MaterializeOptions, Result as CoreResult, SourceReader,
    materialize_layout,
};
use shardpack_format::{BlockCodec, EncryptionKey};
use shardpack_storage::Store;

struct Records(std::vec::IntoIter<EncodedRecord>);

impl SourceReader for Records {
    fn next_record(&mut self) -> CoreResult<Option<EncodedRecord>> {
        Ok(self.0.next())
    }
}

#[tokio::main]
async fn main() {
    let root = std::env::args_os()
        .nth(1)
        .expect("usage: python_fixture OUTPUT_DIRECTORY");
    let root = std::path::PathBuf::from(root);
    std::fs::create_dir_all(&root).unwrap();
    let store = Store::filesystem(&root).unwrap();
    let records = (0_u8..7)
        .map(|value| EncodedRecord {
            sample_id: [value; 16],
            flags: u32::from(value % 2),
            payload: Bytes::from(format!("python-record-{value}")),
        })
        .collect::<Vec<_>>();
    let mut source = Records(records.into_iter());
    let materialized = materialize_layout(
        &mut source,
        [0x51; 32],
        &store,
        &root,
        &MaterializeOptions {
            store_id: "local".into(),
            target_shard_bytes: 48,
            target_block_bytes: 32,
            block_codec: BlockCodec::None,
            object_prefix: "shards".into(),
            layout_prefix: "layouts".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    std::fs::write(root.join("layout-key.txt"), materialized.layout_object_key).unwrap();

    let encrypted_records = (0_u8..7)
        .map(|value| EncodedRecord {
            sample_id: [value; 16],
            flags: u32::from(value % 2),
            payload: Bytes::from(format!("python-record-{value}")),
        })
        .collect::<Vec<_>>();
    let mut encrypted_source = Records(encrypted_records.into_iter());
    let encrypted = materialize_layout(
        &mut encrypted_source,
        [0x52; 32],
        &store,
        &root,
        &MaterializeOptions {
            store_id: "local".into(),
            target_shard_bytes: 48,
            target_block_bytes: 32,
            block_codec: BlockCodec::None,
            object_prefix: "encrypted/shards".into(),
            layout_prefix: "encrypted/layouts".into(),
            encryption: Some(MaterializeEncryption {
                key_id: "test://python-key".into(),
                key: EncryptionKey::new([0x77; 32]),
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    std::fs::write(
        root.join("encrypted-layout-key.txt"),
        encrypted.layout_object_key,
    )
    .unwrap();
}
