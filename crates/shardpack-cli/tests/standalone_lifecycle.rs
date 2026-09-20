use bytes::Bytes;
use shardpack_catalog::Catalog;
use shardpack_core::{
    EncodedRecord, MaterializeEncryption, MaterializeOptions, Result as CoreResult, SourceReader,
    materialize_layout, rebalance_layout_transaction,
};
use shardpack_format::{
    BLOCK_HEADER_SIZE, BlockCodec, BlockHeader, EncryptionKey, SHARD_HEADER_SIZE,
};
use shardpack_runtime::{DatasetReader, Error as RuntimeError, RetryPolicy};
use shardpack_storage::{S3Options, Store};

struct Records(std::vec::IntoIter<EncodedRecord>);

impl SourceReader for Records {
    fn next_record(&mut self) -> CoreResult<Option<EncodedRecord>> {
        Ok(self.0.next())
    }
}

fn records() -> Vec<EncodedRecord> {
    (0..12_u8)
        .map(|value| EncodedRecord {
            sample_id: [value; 16],
            flags: u32::from(value),
            payload: Bytes::from(vec![value; usize::from(value % 4 + 5)]),
        })
        .collect()
}

type RecordView = ([u8; 16], u32, Bytes);

async fn read_all(mut reader: DatasetReader) -> Vec<RecordView> {
    let mut records = Vec::new();
    while let Some(record) = reader.next_record().await.unwrap() {
        records.push((record.sample_id, record.flags, record.payload));
    }
    records
}

async fn exercise_portable_lifecycle(
    store: Store,
    staging: &std::path::Path,
    object_prefix: &str,
    layout_prefix: &str,
    catalog_prefix: &str,
    filesystem_root_for_corruption: Option<&std::path::Path>,
) {
    let key_id = "test://standalone-key";
    let key = EncryptionKey::new([0x91; 32]);
    let expected = records();
    let expected_view = expected
        .iter()
        .map(|record| (record.sample_id, record.flags, record.payload.clone()))
        .collect::<Vec<_>>();
    let mut source = Records(expected.clone().into_iter());
    let snapshot_id = [0x42; 32];

    let source_materialization = materialize_layout(
        &mut source,
        snapshot_id,
        &store,
        staging,
        &MaterializeOptions {
            target_shard_bytes: 24,
            target_block_bytes: 10,
            object_prefix: object_prefix.into(),
            layout_prefix: layout_prefix.into(),
            encryption: Some(MaterializeEncryption {
                key_id: key_id.into(),
                key: key.clone(),
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    drop(source);
    assert!(source_materialization.layout.shards.len() > 1);

    let mut initial = DatasetReader::new(store.clone(), source_materialization.layout.clone())
        .unwrap()
        .with_decryption_key(key_id, key.clone())
        .unwrap();
    let mut observed = Vec::new();
    for _ in 0..5 {
        let record = initial.next_record().await.unwrap().unwrap();
        observed.push((record.sample_id, record.flags, record.payload));
    }
    let checkpoint = initial.checkpoint();
    drop(initial);

    let mut resumed = DatasetReader::resume_encrypted(
        store.clone(),
        source_materialization.layout.clone(),
        &checkpoint,
        key_id,
        key.clone(),
    )
    .await
    .unwrap();
    while let Some(record) = resumed.next_record().await.unwrap() {
        observed.push((record.sample_id, record.flags, record.payload));
    }
    assert_eq!(observed, expected_view);

    let catalog = Catalog::new(store.clone(), catalog_prefix).unwrap();
    catalog
        .switch_preferred(&source_materialization.layout)
        .await
        .unwrap();
    let transaction = rebalance_layout_transaction(
        &store,
        &catalog,
        &source_materialization.layout,
        staging,
        &MaterializeOptions {
            target_shard_bytes: 1024,
            target_block_bytes: 18,
            block_codec: BlockCodec::Zstd,
            builder_version: "standalone-lifecycle-rebalance".into(),
            object_prefix: object_prefix.into(),
            layout_prefix: layout_prefix.into(),
            encryption: Some(MaterializeEncryption {
                key_id: key_id.into(),
                key: key.clone(),
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let target = transaction.materialization.layout;
    assert_eq!(
        target.snapshot_id,
        source_materialization.layout.snapshot_id
    );
    assert_eq!(
        target.content_fingerprint,
        source_materialization.layout.content_fingerprint
    );
    assert_ne!(target.layout_id, source_materialization.layout.layout_id);

    let rebalanced = read_all(
        DatasetReader::new(store.clone(), target.clone())
            .unwrap()
            .with_decryption_key(key_id, key.clone())
            .unwrap(),
    )
    .await;
    assert_eq!(rebalanced, expected_view);

    let Some(filesystem_root) = filesystem_root_for_corruption else {
        return;
    };

    // A block-boundary truncation is structurally valid without a footer, but
    // the runtime must still reject it using the manifest's exact size/hash.
    let shard = &target.shards[0];
    let bytes = store.read_all(&shard.object_uri).await.unwrap();
    let header_start = SHARD_HEADER_SIZE;
    let header_end = header_start + BLOCK_HEADER_SIZE;
    let raw_header: [u8; BLOCK_HEADER_SIZE] = bytes[header_start..header_end].try_into().unwrap();
    let block = BlockHeader::decode(&raw_header).unwrap();
    let first_block_end = header_end + block.stored_payload_bytes as usize;
    assert!(first_block_end < bytes.len());
    std::fs::write(
        filesystem_root.join(&shard.object_uri),
        &bytes[..first_block_end],
    )
    .unwrap();

    let mut truncated = DatasetReader::new(store, target)
        .unwrap()
        .with_decryption_key(key_id, key)
        .unwrap()
        .with_retry_policy(RetryPolicy {
            max_retries: 0,
            ..RetryPolicy::default()
        });
    let error = loop {
        match truncated.next_record().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("manifest identity accepted a block-boundary truncation"),
            Err(error) => break error,
        }
    };
    assert!(matches!(error, RuntimeError::ShardMismatch { .. }));
}

#[tokio::test]
async fn portable_encrypted_dataset_survives_source_loss_resume_and_rebalance() {
    let root = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    let store = Store::filesystem(root.path()).unwrap();
    exercise_portable_lifecycle(
        store,
        staging.path(),
        "shards",
        "layouts",
        "control",
        Some(root.path()),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires an explicitly configured disposable S3-compatible bucket"]
async fn portable_lifecycle_runs_on_s3_compatible_storage() {
    let bucket = std::env::var("SHARDPACK_TEST_S3_BUCKET").expect("test bucket is required");
    let store = Store::s3(&S3Options {
        bucket,
        root: std::env::var("SHARDPACK_TEST_S3_ROOT").ok(),
        endpoint: std::env::var("SHARDPACK_TEST_S3_ENDPOINT").ok(),
        region: std::env::var("AWS_REGION").ok(),
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
        session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        ..S3Options::default()
    })
    .unwrap();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("integration/{}/lifecycle-{nonce}", std::process::id());
    let staging = tempfile::tempdir().unwrap();

    exercise_portable_lifecycle(
        store.clone(),
        staging.path(),
        &format!("{prefix}/shards"),
        &format!("{prefix}/layouts"),
        &format!("{prefix}/control"),
        None,
    )
    .await;

    for key in store.list_keys(&prefix).await.unwrap() {
        store.delete(&key).await.unwrap();
    }
    assert!(store.list_keys(&prefix).await.unwrap().is_empty());
}
