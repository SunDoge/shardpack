use prost::Message;
use shardpack_manifest::{LayoutId, SnapshotId, proto, validate_layout, validate_snapshot};
use shardpack_storage::{Error as StorageError, Store};

use crate::{Error, Result};

#[derive(Clone, Debug)]
pub struct ResolvedDataset {
    pub snapshot: proto::SnapshotManifest,
    pub layout: proto::LayoutManifest,
    pub snapshot_id: SnapshotId,
    pub layout_id: LayoutId,
}

pub async fn publish_snapshot(
    store: &Store,
    prefix: &str,
    manifest: proto::SnapshotManifest,
) -> Result<proto::SnapshotManifest> {
    validate_snapshot(&manifest).map_err(|source| Error::Manifest { source })?;
    let id = SnapshotId::from_slice(&manifest.snapshot_id)
        .map_err(|source| Error::Manifest { source })?;
    let key = snapshot_manifest_key(prefix, &id);
    let encoded = manifest.encode_to_vec();
    let input = futures_util::stream::once(async move { Ok(bytes::Bytes::from(encoded)) });
    match store.write_immutable(&key, Box::pin(input)).await {
        Ok(()) => Ok(manifest),
        Err(StorageError::AlreadyExists { .. }) => {
            let existing = load_snapshot(store, &key).await?;
            if existing.snapshot_id == manifest.snapshot_id {
                Ok(existing)
            } else {
                Err(Error::ObjectIdentityMismatch { key })
            }
        }
        Err(source) => Err(Error::Storage { source }),
    }
}

pub async fn load_snapshot(store: &Store, key: &str) -> Result<proto::SnapshotManifest> {
    load_snapshot_with_limit(store, key, crate::DEFAULT_MAX_MANIFEST_BYTES).await
}

pub async fn load_snapshot_with_limit(
    store: &Store,
    key: &str,
    max_bytes: usize,
) -> Result<proto::SnapshotManifest> {
    let bytes = store
        .read_all_limited(key, max_bytes)
        .await
        .map_err(|source| Error::Storage { source })?;
    let manifest = proto::SnapshotManifest::decode(bytes).map_err(|source| Error::Manifest {
        source: shardpack_manifest::Error::Decode { source },
    })?;
    validate_snapshot(&manifest).map_err(|source| Error::Manifest { source })?;
    Ok(manifest)
}

pub async fn load_layout(store: &Store, key: &str) -> Result<proto::LayoutManifest> {
    load_layout_with_limit(store, key, crate::DEFAULT_MAX_MANIFEST_BYTES).await
}

pub async fn load_layout_with_limit(
    store: &Store,
    key: &str,
    max_bytes: usize,
) -> Result<proto::LayoutManifest> {
    let bytes = store
        .read_all_limited(key, max_bytes)
        .await
        .map_err(|source| Error::Storage { source })?;
    let manifest = proto::LayoutManifest::decode(bytes).map_err(|source| Error::Manifest {
        source: shardpack_manifest::Error::Decode { source },
    })?;
    validate_layout(&manifest).map_err(|source| Error::Manifest { source })?;
    Ok(manifest)
}

pub async fn resolve_dataset(
    store: &Store,
    snapshot_key: &str,
    layout_key: &str,
) -> Result<ResolvedDataset> {
    let snapshot = load_snapshot(store, snapshot_key).await?;
    let layout = load_layout(store, layout_key).await?;
    if layout.snapshot_id != snapshot.snapshot_id {
        return Err(Error::SnapshotLayoutMismatch {
            reason: "layout references another snapshot",
        });
    }
    if layout.portability != snapshot.portability {
        return Err(Error::SnapshotLayoutMismatch {
            reason: "portability differs",
        });
    }
    let expected_records = if let Some(split_name) = layout.split_name.as_deref() {
        snapshot
            .splits
            .iter()
            .find(|split| split.name == split_name)
            .ok_or(Error::SnapshotLayoutMismatch {
                reason: "layout references an unknown split",
            })?
            .record_count
    } else {
        snapshot.logical_record_count
    };
    if layout.total_records != expected_records {
        let explicitly_incomplete = proto::LayoutState::try_from(layout.state)
            == Ok(proto::LayoutState::Incomplete)
            && layout
                .materialization_stats
                .as_ref()
                .is_some_and(|stats| stats.expected_records == expected_records);
        if !explicitly_incomplete {
            return Err(Error::SnapshotLayoutMismatch {
                reason: "record counts differ without an explicit incomplete layout",
            });
        }
    }
    let snapshot_id = SnapshotId::from_slice(&snapshot.snapshot_id)
        .map_err(|source| Error::Manifest { source })?;
    let layout_id =
        LayoutId::from_slice(&layout.layout_id).map_err(|source| Error::Manifest { source })?;
    Ok(ResolvedDataset {
        snapshot,
        layout,
        snapshot_id,
        layout_id,
    })
}

#[must_use]
pub fn snapshot_manifest_key(prefix: &str, id: &SnapshotId) -> String {
    content_addressed_key(prefix, &id.0, "snapshot.pb")
}

#[must_use]
pub fn layout_manifest_key(prefix: &str, id: &LayoutId) -> String {
    content_addressed_key(prefix, &id.0, "layout.pb")
}

fn content_addressed_key(prefix: &str, id: &[u8; 32], suffix: &str) -> String {
    let hash = hex::encode(id);
    format!("{prefix}/{}/{}/{}.{suffix}", &hash[..2], &hash[2..4], hash)
}

#[cfg(test)]
mod tests {
    use shardpack_manifest::{proto, snapshot_id};

    use super::{load_snapshot, publish_snapshot, snapshot_manifest_key};

    #[tokio::test]
    async fn snapshot_publication_is_immutable_and_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let mut snapshot = proto::SnapshotManifest {
            dataset_id: "dataset".into(),
            source: Some(proto::SnapshotSource {
                source: Some(proto::snapshot_source::Source::Imported(
                    proto::ImportedSource { description: None },
                )),
            }),
            record_schema: Some(proto::RecordSchemaDescriptor {
                codec_name: "test".into(),
                codec_version: 1,
                schema_id: vec![1; 32],
                embedded_schema: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            logical_record_count: 0,
            ..Default::default()
        };
        snapshot.snapshot_id = snapshot_id(&snapshot).0.to_vec();
        let published = publish_snapshot(&store, "snapshots", snapshot.clone())
            .await
            .unwrap();
        let id = shardpack_manifest::SnapshotId::from_slice(&snapshot.snapshot_id).unwrap();
        let key = snapshot_manifest_key("snapshots", &id);
        assert_eq!(load_snapshot(&store, &key).await.unwrap(), published);
        assert_eq!(
            publish_snapshot(&store, "snapshots", snapshot)
                .await
                .unwrap(),
            published
        );
    }
}
