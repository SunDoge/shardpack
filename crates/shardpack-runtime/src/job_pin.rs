use shardpack_catalog::Catalog;
use shardpack_manifest::proto;

use crate::{Error, Result};

/// A persistent GC root for one exact training layout.
///
/// Call [`Self::release`] after a clean job shutdown. Deliberately, dropping
/// this value does not remove the pin: after a process crash the durable pin
/// protects resumable work until an operator or recovery coordinator unpins it.
#[derive(Debug)]
#[must_use = "a job pin must be retained for the lifetime of the training job"]
pub struct JobPin {
    catalog: Catalog,
    pin_id: String,
}

impl JobPin {
    pub async fn acquire(
        catalog: Catalog,
        pin_id: impl Into<String>,
        layout: &proto::LayoutManifest,
        expires_at_unix_seconds: Option<i64>,
    ) -> Result<Self> {
        shardpack_manifest::validate_layout(layout).map_err(|source| Error::Manifest { source })?;
        let pin_id = pin_id.into();
        catalog
            .pin(proto::LayoutPin {
                pin_id: pin_id.clone(),
                snapshot_id: layout.snapshot_id.clone(),
                layout_id: layout.layout_id.clone(),
                reason: proto::PinReason::RunningJob as i32,
                created_at: Some(now_timestamp()),
                expires_at: expires_at_unix_seconds.map(timestamp),
            })
            .await
            .map_err(|source| Error::Catalog { source })?;
        Ok(Self { catalog, pin_id })
    }

    #[must_use]
    pub fn pin_id(&self) -> &str {
        &self.pin_id
    }

    /// Removes the persistent root after the trainer and all readers have
    /// stopped using the layout.
    pub async fn release(self) -> Result<()> {
        self.catalog
            .unpin(&self.pin_id)
            .await
            .map_err(|source| Error::Catalog { source })
    }
}

fn now_timestamp() -> prost_types::Timestamp {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX);
    timestamp(seconds)
}

const fn timestamp(seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos: 0 }
}

#[cfg(test)]
mod tests {
    use shardpack_format::ContentFingerprint;
    use shardpack_manifest::{layout_id, proto};

    use super::JobPin;

    #[tokio::test]
    async fn persists_until_explicit_clean_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let catalog = shardpack_catalog::Catalog::new(store, "catalog").unwrap();
        let fingerprint = ContentFingerprint::default();
        let mut layout = proto::LayoutManifest {
            snapshot_id: vec![1; 32],
            builder_version: "test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1,
                target_block_bytes: 1,
                block_codec: "none".into(),
                shuffle_seed: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            content_fingerprint: Some(proto::ContentFingerprint {
                record_count: 0,
                accumulator_a: fingerprint.accumulator_a.to_vec(),
                accumulator_b: fingerprint.accumulator_b.to_vec(),
            }),
            created_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();

        let pin = JobPin::acquire(catalog.clone(), "job-7", &layout, None)
            .await
            .unwrap();
        assert_eq!(pin.pin_id(), "job-7");
        assert_eq!(catalog.pins().await.unwrap().len(), 1);
        pin.release().await.unwrap();
        assert!(catalog.pins().await.unwrap().is_empty());
    }
}
