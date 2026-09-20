use shardpack_format::{AsyncShardReader, Blake3Reader, ContentFingerprint};
use shardpack_manifest::{SnapshotId, layout_id, proto, snapshot_id};
use shardpack_storage::Store;

use crate::{
    Error, Result,
    materialize::{manifest_object_key, now_timestamp, proto_fingerprint, publish_layout},
    publish_snapshot, snapshot_manifest_key,
};

#[derive(Clone, Debug)]
pub struct ImportOptions {
    pub dataset_id: String,
    pub description: Option<String>,
    pub record_schema: proto::RecordSchemaDescriptor,
    pub shard_object_uris: Vec<String>,
    pub snapshot_prefix: String,
    pub layout_prefix: String,
    /// Describes the imported packing. It does not constrain codecs found in
    /// individual block headers.
    pub layout_recipe: proto::LayoutRecipe,
    pub builder_version: String,
}

#[derive(Clone, Debug)]
pub struct ImportedDataset {
    pub snapshot: proto::SnapshotManifest,
    pub snapshot_object_key: String,
    pub layout: proto::LayoutManifest,
    pub layout_object_key: String,
}

/// Imports existing immutable shards without rewriting them. Every object is
/// consumed once sequentially; block checksums, directory structure, aggregate
/// content and exact-byte BLAKE3 identity are derived from that pass.
pub async fn import_shardpack(store: &Store, options: &ImportOptions) -> Result<ImportedDataset> {
    import_shardpack_inner(store, options, None).await
}

pub async fn import_encrypted_shardpack(
    store: &Store,
    options: &ImportOptions,
    key_id: &str,
    key: shardpack_format::EncryptionKey,
) -> Result<ImportedDataset> {
    if key_id.is_empty() {
        return Err(Error::InvalidOption {
            field: "encryption.key_id",
            reason: "must not be empty",
        });
    }
    import_shardpack_inner(store, options, Some((key_id, key))).await
}

async fn import_shardpack_inner(
    store: &Store,
    options: &ImportOptions,
    encryption: Option<(&str, shardpack_format::EncryptionKey)>,
) -> Result<ImportedDataset> {
    validate_options(options)?;
    let mut shards = Vec::with_capacity(options.shard_object_uris.len());
    let mut total_fingerprint = ContentFingerprint::default();
    let mut total_bytes = 0_u64;

    for object_uri in &options.shard_object_uris {
        let object = store
            .open_reader(object_uri)
            .await
            .map_err(|source| Error::Storage { source })?;
        let mut reader = AsyncShardReader::with_limits_and_key(
            Blake3Reader::new(object),
            shardpack_format::ReaderLimits::default(),
            encryption.as_ref().map(|(_, key)| key.clone()),
        )
        .await
        .map_err(|source| Error::Format { source })?;
        while reader
            .next_record()
            .await
            .map_err(|source| Error::Format { source })?
            .is_some()
        {}
        let fingerprint = reader.content_fingerprint();
        let byte_size = reader.input_ref().bytes_read();
        let shard_id = reader.input_ref().digest();
        let block_count = reader.block_count();
        if fingerprint.record_count == 0 || block_count == 0 {
            return Err(Error::InvalidOption {
                field: "shard_object_uris",
                reason: "empty shards cannot be published",
            });
        }
        total_fingerprint.merge(&fingerprint);
        total_bytes = total_bytes
            .checked_add(byte_size)
            .ok_or(Error::InvalidOption {
                field: "shard_object_uris",
                reason: "aggregate byte size overflow",
            })?;
        shards.push(proto::ShardMeta {
            shard_id: shard_id.to_vec(),
            object_uri: object_uri.clone(),
            byte_size,
            record_count: fingerprint.record_count,
            block_count,
            content_fingerprint: Some(proto_fingerprint(fingerprint)),
        });
    }

    let mut snapshot = proto::SnapshotManifest {
        dataset_id: options.dataset_id.clone(),
        source: Some(proto::SnapshotSource {
            source: Some(proto::snapshot_source::Source::Imported(
                proto::ImportedSource {
                    description: options.description.clone(),
                },
            )),
        }),
        record_schema: Some(options.record_schema.clone()),
        portability: proto::Portability::SelfContained as i32,
        durability: proto::Durability::Durable as i32,
        logical_record_count: total_fingerprint.record_count,
        materialization_stats: Some(proto::MaterializationStats {
            expected_records: total_fingerprint.record_count,
            written_records: total_fingerprint.record_count,
            skipped_records: 0,
        }),
        created_at: Some(now_timestamp()),
        ..Default::default()
    };
    snapshot.snapshot_id = snapshot_id(&snapshot).0.to_vec();
    let snapshot = publish_snapshot(store, &options.snapshot_prefix, snapshot).await?;
    let typed_snapshot_id = SnapshotId::from_slice(&snapshot.snapshot_id)
        .map_err(|source| Error::Manifest { source })?;
    let snapshot_object_key = snapshot_manifest_key(&options.snapshot_prefix, &typed_snapshot_id);

    let mut layout = proto::LayoutManifest {
        snapshot_id: snapshot.snapshot_id.clone(),
        builder_version: options.builder_version.clone(),
        recipe: Some(options.layout_recipe.clone()),
        portability: snapshot.portability,
        durability: snapshot.durability,
        total_records: total_fingerprint.record_count,
        total_bytes,
        content_fingerprint: Some(proto_fingerprint(total_fingerprint)),
        shards,
        created_at: Some(now_timestamp()),
        state: proto::LayoutState::Complete as i32,
        materialization_stats: Some(proto::MaterializationStats {
            expected_records: total_fingerprint.record_count,
            written_records: total_fingerprint.record_count,
            skipped_records: 0,
        }),
        encryption: encryption
            .as_ref()
            .map(|(key_id, _)| proto::EncryptionDescriptor {
                algorithm: proto::EncryptionAlgorithm::Aes256Gcm as i32,
                key_id: (*key_id).to_owned(),
            }),
        ..Default::default()
    };
    layout.layout_id = layout_id(&layout).0.to_vec();
    shardpack_manifest::validate_layout(&layout).map_err(|source| Error::Manifest { source })?;
    let layout_id: [u8; 32] = layout
        .layout_id
        .as_slice()
        .try_into()
        .expect("validated layout ID");
    let layout_object_key = manifest_object_key(&options.layout_prefix, &layout_id);
    let layout = publish_layout(store, &layout_object_key, layout).await?;

    Ok(ImportedDataset {
        snapshot,
        snapshot_object_key,
        layout,
        layout_object_key,
    })
}

/// Imports while holding the shared publication/sweep barrier.
pub async fn import_shardpack_coordinated(
    store: &Store,
    catalog: &shardpack_catalog::Catalog,
    options: &ImportOptions,
) -> Result<ImportedDataset> {
    let lease = catalog
        .acquire_barrier(shardpack_catalog::new_owner_id(), "standalone-import")
        .await
        .map_err(|source| Error::Catalog { source })?;
    let operation = import_shardpack(store, options).await;
    crate::materialize::finish_coordinated(catalog, &lease, operation).await
}

fn validate_options(options: &ImportOptions) -> Result<()> {
    if options.shard_object_uris.is_empty() {
        return Err(Error::InvalidOption {
            field: "shard_object_uris",
            reason: "at least one shard is required",
        });
    }
    if options.snapshot_prefix.is_empty() || options.layout_prefix.is_empty() {
        return Err(Error::InvalidOption {
            field: "manifest prefix",
            reason: "must not be empty",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::stream;
    use shardpack_format::{ShardWriter, WriterOptions};

    use super::{ImportOptions, import_shardpack};
    use shardpack_manifest::proto;

    #[tokio::test]
    async fn imports_existing_shards_as_a_durable_portable_dataset() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([1; 16], 0, b"one").unwrap();
        writer.add_record([2; 16], 0, b"two").unwrap();
        let (bytes, expected) = writer.finish().unwrap();
        store
            .write_immutable(
                "existing/a.spk",
                Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) })),
            )
            .await
            .unwrap();

        let imported = import_shardpack(
            &store,
            &ImportOptions {
                dataset_id: "standalone".into(),
                description: Some("only surviving copy".into()),
                record_schema: proto::RecordSchemaDescriptor {
                    codec_name: "test".into(),
                    codec_version: 1,
                    schema_id: vec![5; 32],
                    embedded_schema: Some(vec![1, 2, 3]),
                },
                shard_object_uris: vec!["existing/a.spk".into()],
                snapshot_prefix: "snapshots".into(),
                layout_prefix: "layouts".into(),
                layout_recipe: proto::LayoutRecipe {
                    target_shard_bytes: 1024,
                    target_block_bytes: 1024,
                    block_codec: "none".into(),
                    shuffle_seed: None,
                },
                builder_version: "import-test".into(),
            },
        )
        .await
        .unwrap();

        assert_eq!(imported.snapshot.logical_record_count, 2);
        assert_eq!(
            imported.snapshot.portability,
            proto::Portability::SelfContained as i32
        );
        assert_eq!(
            imported.snapshot.durability,
            proto::Durability::Durable as i32
        );
        assert_eq!(imported.layout.shards[0].shard_id, expected.shard_id);
        store
            .operator()
            .stat(&imported.snapshot_object_key)
            .await
            .unwrap();
        store
            .operator()
            .stat(&imported.layout_object_key)
            .await
            .unwrap();
    }
}
