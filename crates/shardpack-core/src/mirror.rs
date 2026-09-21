use prost::Message;
use shardpack_format::ContentFingerprint;
use shardpack_manifest::proto;
use shardpack_storage::{Error as StorageError, Store};

use crate::{AsyncSourceReader, Error, Result, ShardPackLayoutSource};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MirrorResult {
    pub shard_objects: usize,
    pub total_bytes: u64,
    pub snapshot_object_key: String,
    pub layout_object_key: String,
}

/// Mirrors one immutable snapshot/layout pair without decoding or re-encoding
/// authoritative manifests. Shards are published and fully verified first;
/// manifests become visible at the destination only after validation succeeds.
pub async fn mirror_dataset(
    source: &Store,
    destination: &Store,
    snapshot_object_key: &str,
    layout_object_key: &str,
) -> Result<MirrorResult> {
    let snapshot_bytes = source
        .read_all_limited(snapshot_object_key, crate::DEFAULT_MAX_MANIFEST_BYTES)
        .await
        .map_err(|source| Error::Storage { source })?;
    let layout_bytes = source
        .read_all_limited(layout_object_key, crate::DEFAULT_MAX_MANIFEST_BYTES)
        .await
        .map_err(|source| Error::Storage { source })?;
    let snapshot = proto::SnapshotManifest::decode(snapshot_bytes.clone()).map_err(|source| {
        Error::Manifest {
            source: shardpack_manifest::Error::Decode { source },
        }
    })?;
    let layout =
        proto::LayoutManifest::decode(layout_bytes.clone()).map_err(|source| Error::Manifest {
            source: shardpack_manifest::Error::Decode { source },
        })?;
    shardpack_manifest::validate_snapshot(&snapshot)
        .map_err(|source| Error::Manifest { source })?;
    shardpack_manifest::validate_layout(&layout).map_err(|source| Error::Manifest { source })?;
    if snapshot.snapshot_id != layout.snapshot_id {
        return Err(Error::MirrorMismatch {
            reason: "layout belongs to another snapshot",
        });
    }

    for shard in &layout.shards {
        let location = shardpack_manifest::object_location(shard);
        let stream = source
            .stream(&location.object_key)
            .await
            .map_err(|source| Error::Storage { source })?;
        match destination
            .write_immutable(&location.object_key, stream)
            .await
        {
            Ok(()) | Err(StorageError::AlreadyExists { .. }) => {}
            Err(source) => return Err(Error::Storage { source }),
        }
    }

    let mut reader = ShardPackLayoutSource::new(destination.clone(), layout.clone())?;
    let mut fingerprint = ContentFingerprint::default();
    while let Some(record) = reader.next_record().await? {
        fingerprint.insert(&record.sample_id, &record.payload);
    }
    let expected = layout
        .content_fingerprint
        .as_ref()
        .expect("validated content fingerprint");
    if fingerprint.record_count != layout.total_records
        || fingerprint.record_count != expected.record_count
        || fingerprint.accumulator_a.as_slice() != expected.accumulator_a
        || fingerprint.accumulator_b.as_slice() != expected.accumulator_b
    {
        return Err(Error::MirrorMismatch {
            reason: "destination records differ from layout fingerprint",
        });
    }

    publish_exact(destination, snapshot_object_key, snapshot_bytes).await?;
    publish_exact(destination, layout_object_key, layout_bytes).await?;
    Ok(MirrorResult {
        shard_objects: layout.shards.len(),
        total_bytes: layout.total_bytes,
        snapshot_object_key: snapshot_object_key.into(),
        layout_object_key: layout_object_key.into(),
    })
}

async fn publish_exact(store: &Store, key: &str, bytes: bytes::Bytes) -> Result<()> {
    match store.write_bytes_immutable(key, bytes.clone()).await {
        Ok(()) => Ok(()),
        Err(StorageError::AlreadyExists { .. }) => {
            let existing = store
                .read_all_limited(key, bytes.len())
                .await
                .map_err(|source| Error::Storage { source })?;
            if existing == bytes {
                Ok(())
            } else {
                Err(Error::ObjectIdentityMismatch { key: key.into() })
            }
        }
        Err(source) => Err(Error::Storage { source }),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::stream;
    use prost::Message;
    use shardpack_format::{ShardWriter, WriterOptions};
    use shardpack_manifest::{layout_id, proto, snapshot_id};

    use super::mirror_dataset;

    #[tokio::test]
    async fn mirror_survives_complete_source_loss_and_is_idempotent() {
        let source_root = tempfile::tempdir().unwrap();
        let destination_root = tempfile::tempdir().unwrap();
        let source = shardpack_storage::Store::filesystem(source_root.path()).unwrap();
        let destination = shardpack_storage::Store::filesystem(destination_root.path()).unwrap();
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([7; 16], 0, b"portable").unwrap();
        let (shard_bytes, summary) = writer.finish().unwrap();
        let shard_key = "shards/portable.spk";
        source
            .write_immutable(
                shard_key,
                Box::pin(stream::once(async move { Ok(Bytes::from(shard_bytes)) })),
            )
            .await
            .unwrap();
        let mut snapshot = proto::SnapshotManifest {
            dataset_id: "portable".into(),
            source: Some(proto::SnapshotSource {
                source: Some(proto::snapshot_source::Source::Imported(
                    proto::ImportedSource { description: None },
                )),
            }),
            record_schema: Some(proto::RecordSchemaDescriptor {
                codec_name: "opaque".into(),
                codec_version: 1,
                schema_id: vec![1; 32],
                embedded_schema: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            logical_record_count: 1,
            ..Default::default()
        };
        snapshot.snapshot_id = snapshot_id(&snapshot).0.to_vec();
        let fingerprint = proto::ContentFingerprint {
            record_count: 1,
            accumulator_a: summary.content_fingerprint.accumulator_a.to_vec(),
            accumulator_b: summary.content_fingerprint.accumulator_b.to_vec(),
        };
        let mut layout = proto::LayoutManifest {
            snapshot_id: snapshot.snapshot_id.clone(),
            builder_version: "test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1024,
                target_block_bytes: 1024,
                block_codec: "none".into(),
                shuffle_seed: None,
                max_shard_bytes: None,
            }),
            portability: snapshot.portability,
            durability: snapshot.durability,
            total_records: 1,
            total_bytes: summary.byte_size,
            content_fingerprint: Some(fingerprint.clone()),
            shards: vec![proto::ShardMeta {
                shard_id: summary.shard_id.to_vec(),
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: shard_key.into(),
                }),
                byte_size: summary.byte_size,
                record_count: 1,
                block_count: summary.block_count,
                content_fingerprint: Some(fingerprint),
            }],
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();
        source
            .write_bytes_immutable(
                "snapshots/portable.snapshot.pb",
                snapshot.encode_to_vec().into(),
            )
            .await
            .unwrap();
        source
            .write_bytes_immutable("layouts/portable.layout.pb", layout.encode_to_vec().into())
            .await
            .unwrap();

        for _ in 0..2 {
            mirror_dataset(
                &source,
                &destination,
                "snapshots/portable.snapshot.pb",
                "layouts/portable.layout.pb",
            )
            .await
            .unwrap();
        }
        drop(source);
        source_root.close().unwrap();
        let mut reader = crate::ShardPackLayoutSource::new(destination, layout).unwrap();
        assert_eq!(
            crate::AsyncSourceReader::next_record(&mut reader)
                .await
                .unwrap()
                .unwrap()
                .payload,
            "portable"
        );
    }
}
