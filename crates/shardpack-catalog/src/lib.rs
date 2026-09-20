//! Persistent object-store control plane built from immutable CAS-like events.

use bytes::Bytes;
use futures_util::stream;
use prost::Message;
use shardpack_manifest::{LayoutId, SnapshotId, proto};
use shardpack_storage::{Error as StorageError, Store};
use snafu::Snafu;
use std::sync::atomic::{AtomicU64, Ordering};

const MAX_CONTROL_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("storage operation failed: {source}"))]
    Storage { source: shardpack_storage::Error },
    #[snafu(display("manifest operation failed: {source}"))]
    Manifest { source: shardpack_manifest::Error },
    #[snafu(display("catalog protobuf decode failed for {key:?}: {source}"))]
    Decode {
        key: String,
        source: prost::DecodeError,
    },
    #[snafu(display("invalid catalog data in {key:?}: {reason}"))]
    Invalid { key: String, reason: &'static str },
    #[snafu(display("preferred-layout generation changed concurrently"))]
    Conflict,
    #[snafu(display("pin {pin_id:?} already exists"))]
    PinAlreadyExists { pin_id: String },
    #[snafu(display("catalog coordination barrier is held by another operation"))]
    BarrierBusy,
    #[snafu(display("catalog coordination barrier is not owned by {owner_id:?}"))]
    BarrierNotOwned { owner_id: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug)]
pub struct Catalog {
    store: Store,
    prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BarrierLease {
    pub owner_id: String,
    pub purpose: String,
}

static OWNER_COUNTER: AtomicU64 = AtomicU64::new(1);

#[must_use]
pub fn new_owner_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "p{}-{nanos}-{}",
        std::process::id(),
        OWNER_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

impl Catalog {
    pub fn new(store: Store, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        if !valid_component_path(&prefix) {
            return Err(Error::Invalid {
                key: prefix,
                reason: "catalog prefix must be a normalized relative path",
            });
        }
        Ok(Self { store, prefix })
    }

    pub async fn acquire_barrier(
        &self,
        owner_id: impl Into<String>,
        purpose: impl Into<String>,
    ) -> Result<BarrierLease> {
        let owner_id = owner_id.into();
        let purpose = purpose.into();
        validate_pin_id(&owner_id).map_err(|reason| Error::Invalid {
            key: owner_id.clone(),
            reason,
        })?;
        if purpose.is_empty() {
            return Err(Error::Invalid {
                key: purpose,
                reason: "barrier purpose must not be empty",
            });
        }
        let value = proto::CoordinationBarrier {
            owner_id: owner_id.clone(),
            purpose: purpose.clone(),
            created_at: Some(now_timestamp()),
        };
        match self.write_message(&barrier_key(&self.prefix), &value).await {
            Ok(()) => Ok(BarrierLease { owner_id, purpose }),
            Err(Error::Storage {
                source: StorageError::AlreadyExists { .. },
            }) => Err(Error::BarrierBusy),
            Err(error) => Err(error),
        }
    }

    pub async fn release_barrier(&self, lease: &BarrierLease) -> Result<()> {
        self.assert_barrier(lease).await?;
        self.store
            .delete(&barrier_key(&self.prefix))
            .await
            .map_err(|source| Error::Storage { source })
    }

    pub async fn assert_barrier(&self, lease: &BarrierLease) -> Result<()> {
        let key = barrier_key(&self.prefix);
        let current: proto::CoordinationBarrier = match self.read_message(&key).await {
            Ok(value) => value,
            Err(Error::Storage {
                source: StorageError::NotFound { .. },
            }) => {
                return Err(Error::BarrierNotOwned {
                    owner_id: lease.owner_id.clone(),
                });
            }
            Err(error) => return Err(error),
        };
        if current.owner_id != lease.owner_id || current.purpose != lease.purpose {
            return Err(Error::BarrierNotOwned {
                owner_id: lease.owner_id.clone(),
            });
        }
        Ok(())
    }

    /// Administrative crash recovery. The caller must provide the exact owner
    /// recorded in the barrier object; no wall-clock expiry is guessed.
    pub async fn recover_barrier(&self, expected_owner_id: &str) -> Result<()> {
        let key = barrier_key(&self.prefix);
        let current: proto::CoordinationBarrier = self.read_message(&key).await?;
        if current.owner_id != expected_owner_id {
            return Err(Error::BarrierNotOwned {
                owner_id: expected_owner_id.to_owned(),
            });
        }
        self.store
            .delete(&key)
            .await
            .map_err(|source| Error::Storage { source })
    }

    /// Atomically advances the preferred layout for one snapshot. Immutable
    /// generation keys make `if-not-exists` equivalent to compare-and-swap.
    pub async fn switch_preferred(
        &self,
        layout: &proto::LayoutManifest,
    ) -> Result<proto::PreferredLayoutEvent> {
        let lease = self
            .acquire_barrier(new_owner_id(), "preferred-switch")
            .await?;
        let result = self.switch_preferred_guarded(&lease, layout).await;
        self.finish_guarded(&lease, result).await
    }

    pub async fn switch_preferred_guarded(
        &self,
        lease: &BarrierLease,
        layout: &proto::LayoutManifest,
    ) -> Result<proto::PreferredLayoutEvent> {
        self.assert_barrier(lease).await?;
        self.switch_preferred_inner(layout, None).await
    }

    /// Switches only if the current preferred layout equals `expected`.
    /// `None` means that no preferred layout may exist yet.
    pub async fn compare_and_switch_preferred(
        &self,
        layout: &proto::LayoutManifest,
        expected: Option<LayoutId>,
    ) -> Result<proto::PreferredLayoutEvent> {
        let lease = self
            .acquire_barrier(new_owner_id(), "preferred-cas")
            .await?;
        let result = self
            .compare_and_switch_preferred_guarded(&lease, layout, expected)
            .await;
        self.finish_guarded(&lease, result).await
    }

    pub async fn compare_and_switch_preferred_guarded(
        &self,
        lease: &BarrierLease,
        layout: &proto::LayoutManifest,
        expected: Option<LayoutId>,
    ) -> Result<proto::PreferredLayoutEvent> {
        self.assert_barrier(lease).await?;
        self.switch_preferred_inner(layout, Some(expected)).await
    }

    async fn switch_preferred_inner(
        &self,
        layout: &proto::LayoutManifest,
        expected: Option<Option<LayoutId>>,
    ) -> Result<proto::PreferredLayoutEvent> {
        shardpack_manifest::validate_layout(layout).map_err(|source| Error::Manifest { source })?;
        let snapshot_id = SnapshotId::from_slice(&layout.snapshot_id)
            .map_err(|source| Error::Manifest { source })?;
        let layout_id =
            LayoutId::from_slice(&layout.layout_id).map_err(|source| Error::Manifest { source })?;
        let current = self.preferred(snapshot_id).await?;
        if current
            .as_ref()
            .is_some_and(|event| event.layout_id == layout.layout_id)
        {
            return Ok(current.expect("checked as some"));
        }
        if let Some(expected) = expected {
            let actual = current
                .as_ref()
                .map(|value| LayoutId::from_slice(&value.layout_id))
                .transpose()
                .map_err(|source| Error::Manifest { source })?;
            if actual != expected {
                return Err(Error::Conflict);
            }
        }
        let generation = current.as_ref().map_or(1, |value| value.generation + 1);
        let event = proto::PreferredLayoutEvent {
            snapshot_id: snapshot_id.0.to_vec(),
            generation,
            previous_layout_id: current.map(|value| value.layout_id),
            layout_id: layout_id.0.to_vec(),
            created_at: Some(now_timestamp()),
        };
        let key = preferred_event_key(&self.prefix, snapshot_id, generation);
        match self.write_message(&key, &event).await {
            Ok(()) => Ok(event),
            Err(Error::Storage {
                source: StorageError::AlreadyExists { .. },
            }) => Err(Error::Conflict),
            Err(error) => Err(error),
        }
    }

    pub async fn preferred(
        &self,
        snapshot_id: SnapshotId,
    ) -> Result<Option<proto::PreferredLayoutEvent>> {
        let prefix = preferred_event_prefix(&self.prefix, snapshot_id);
        let keys = self
            .store
            .list_keys(&prefix)
            .await
            .map_err(|source| Error::Storage { source })?;
        let mut previous: Option<proto::PreferredLayoutEvent> = None;
        for key in keys {
            let event: proto::PreferredLayoutEvent = self.read_message(&key).await?;
            validate_event(&key, &event, snapshot_id, previous.as_ref())?;
            previous = Some(event);
        }
        Ok(previous)
    }

    pub async fn pin(&self, pin: proto::LayoutPin) -> Result<()> {
        let lease = self.acquire_barrier(new_owner_id(), "layout-pin").await?;
        let result = self.pin_guarded(&lease, pin).await;
        self.finish_guarded(&lease, result).await
    }

    pub async fn pin_guarded(&self, lease: &BarrierLease, pin: proto::LayoutPin) -> Result<()> {
        self.assert_barrier(lease).await?;
        validate_pin("new pin", &pin)?;
        let key = pin_key(&self.prefix, &pin.pin_id);
        match self.write_message(&key, &pin).await {
            Ok(()) => Ok(()),
            Err(Error::Storage {
                source: StorageError::AlreadyExists { .. },
            }) => Err(Error::PinAlreadyExists { pin_id: pin.pin_id }),
            Err(error) => Err(error),
        }
    }

    pub async fn unpin(&self, pin_id: &str) -> Result<()> {
        let lease = self.acquire_barrier(new_owner_id(), "layout-unpin").await?;
        let result = self.unpin_guarded(&lease, pin_id).await;
        self.finish_guarded(&lease, result).await
    }

    pub async fn unpin_guarded(&self, lease: &BarrierLease, pin_id: &str) -> Result<()> {
        self.assert_barrier(lease).await?;
        validate_pin_id(pin_id).map_err(|reason| Error::Invalid {
            key: pin_id.to_owned(),
            reason,
        })?;
        self.store
            .delete(&pin_key(&self.prefix, pin_id))
            .await
            .map_err(|source| Error::Storage { source })
    }

    pub async fn pins(&self) -> Result<Vec<proto::LayoutPin>> {
        let prefix = format!("{}/pins/", self.prefix);
        let keys = self
            .store
            .list_keys(&prefix)
            .await
            .map_err(|source| Error::Storage { source })?;
        let mut pins = Vec::with_capacity(keys.len());
        for key in keys {
            let pin = self.read_message(&key).await?;
            validate_pin(&key, &pin)?;
            pins.push(pin);
        }
        Ok(pins)
    }

    /// Records retirement after a replacement has become preferred. The
    /// fixed source-layout key makes the operation idempotent.
    pub async fn retire(
        &self,
        source: &proto::LayoutManifest,
        replacement: LayoutId,
    ) -> Result<proto::LayoutRetirement> {
        let lease = self
            .acquire_barrier(new_owner_id(), "layout-retirement")
            .await?;
        let result = self.retire_guarded(&lease, source, replacement).await;
        self.finish_guarded(&lease, result).await
    }

    pub async fn retire_guarded(
        &self,
        lease: &BarrierLease,
        source: &proto::LayoutManifest,
        replacement: LayoutId,
    ) -> Result<proto::LayoutRetirement> {
        self.assert_barrier(lease).await?;
        shardpack_manifest::validate_layout(source).map_err(|source| Error::Manifest { source })?;
        let snapshot_id = SnapshotId::from_slice(&source.snapshot_id)
            .map_err(|source| Error::Manifest { source })?;
        let layout_id =
            LayoutId::from_slice(&source.layout_id).map_err(|source| Error::Manifest { source })?;
        if layout_id == replacement {
            return Err(Error::Invalid {
                key: layout_id.to_string(),
                reason: "layout cannot replace itself",
            });
        }
        let preferred = self.preferred(snapshot_id).await?.ok_or(Error::Conflict)?;
        if preferred.layout_id != replacement.0 {
            return Err(Error::Conflict);
        }
        let retirement = proto::LayoutRetirement {
            snapshot_id: snapshot_id.0.to_vec(),
            layout_id: layout_id.0.to_vec(),
            replacement_layout_id: replacement.0.to_vec(),
            retired_at: Some(now_timestamp()),
        };
        let key = retirement_key(&self.prefix, layout_id);
        match self.write_message(&key, &retirement).await {
            Ok(()) => Ok(retirement),
            Err(Error::Storage {
                source: StorageError::AlreadyExists { .. },
            }) => {
                let existing: proto::LayoutRetirement = self.read_message(&key).await?;
                validate_retirement(&key, &existing)?;
                if existing.snapshot_id == retirement.snapshot_id
                    && existing.layout_id == retirement.layout_id
                    && existing.replacement_layout_id == retirement.replacement_layout_id
                {
                    Ok(existing)
                } else {
                    Err(Error::Conflict)
                }
            }
            Err(error) => Err(error),
        }
    }

    pub async fn retirements(&self) -> Result<Vec<proto::LayoutRetirement>> {
        let prefix = format!("{}/retirements/", self.prefix);
        let keys = self
            .store
            .list_keys(&prefix)
            .await
            .map_err(|source| Error::Storage { source })?;
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let value = self.read_message(&key).await?;
            validate_retirement(&key, &value)?;
            values.push(value);
        }
        Ok(values)
    }

    async fn write_message<M: Message>(&self, key: &str, message: &M) -> Result<()> {
        let bytes = message.encode_to_vec();
        self.store
            .write_immutable(
                key,
                Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) })),
            )
            .await
            .map_err(|source| Error::Storage { source })
    }

    async fn read_message<M: Message + Default>(&self, key: &str) -> Result<M> {
        let bytes = self
            .store
            .read_all_limited(key, MAX_CONTROL_MESSAGE_BYTES)
            .await
            .map_err(|source| Error::Storage { source })?;
        M::decode(bytes).map_err(|source| Error::Decode {
            key: key.to_owned(),
            source,
        })
    }

    async fn finish_guarded<T>(&self, lease: &BarrierLease, operation: Result<T>) -> Result<T> {
        let release = self.release_barrier(lease).await;
        match operation {
            Err(error) => {
                let _ = release;
                Err(error)
            }
            Ok(value) => {
                release?;
                Ok(value)
            }
        }
    }
}

fn validate_event(
    key: &str,
    event: &proto::PreferredLayoutEvent,
    snapshot_id: SnapshotId,
    previous: Option<&proto::PreferredLayoutEvent>,
) -> Result<()> {
    if event.snapshot_id != snapshot_id.0 {
        return Err(Error::Invalid {
            key: key.to_owned(),
            reason: "snapshot ID differs from event stream",
        });
    }
    LayoutId::from_slice(&event.layout_id).map_err(|source| Error::Manifest { source })?;
    let expected_generation = previous.map_or(1, |value| value.generation + 1);
    if event.generation != expected_generation {
        return Err(Error::Invalid {
            key: key.to_owned(),
            reason: "generation is not contiguous",
        });
    }
    let expected_previous = previous.map(|value| value.layout_id.as_slice());
    if event.previous_layout_id.as_deref() != expected_previous {
        return Err(Error::Invalid {
            key: key.to_owned(),
            reason: "previous layout ID does not match prior generation",
        });
    }
    if event.created_at.is_none() {
        return Err(Error::Invalid {
            key: key.to_owned(),
            reason: "creation timestamp is missing",
        });
    }
    Ok(())
}

fn validate_pin(key: &str, pin: &proto::LayoutPin) -> Result<()> {
    validate_pin_id(&pin.pin_id).map_err(|reason| Error::Invalid {
        key: key.to_owned(),
        reason,
    })?;
    SnapshotId::from_slice(&pin.snapshot_id).map_err(|source| Error::Manifest { source })?;
    LayoutId::from_slice(&pin.layout_id).map_err(|source| Error::Manifest { source })?;
    if proto::PinReason::try_from(pin.reason)
        .ok()
        .is_none_or(|reason| reason == proto::PinReason::Unspecified)
    {
        return Err(Error::Invalid {
            key: key.to_owned(),
            reason: "pin reason must be explicit",
        });
    }
    if pin.created_at.is_none() {
        return Err(Error::Invalid {
            key: key.to_owned(),
            reason: "creation timestamp is missing",
        });
    }
    Ok(())
}

fn validate_retirement(key: &str, value: &proto::LayoutRetirement) -> Result<()> {
    SnapshotId::from_slice(&value.snapshot_id).map_err(|source| Error::Manifest { source })?;
    let source =
        LayoutId::from_slice(&value.layout_id).map_err(|source| Error::Manifest { source })?;
    let replacement = LayoutId::from_slice(&value.replacement_layout_id)
        .map_err(|source| Error::Manifest { source })?;
    if source == replacement || value.retired_at.is_none() {
        return Err(Error::Invalid {
            key: key.to_owned(),
            reason: "retirement requires distinct layouts and a timestamp",
        });
    }
    Ok(())
}

fn validate_pin_id(pin_id: &str) -> std::result::Result<(), &'static str> {
    if pin_id.is_empty()
        || !pin_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Err("pin ID must contain only ASCII letters, digits, '.', '-' or '_'")
    } else {
        Ok(())
    }
}

fn valid_component_path(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.ends_with('/')
        && value
            .split('/')
            .all(|part| !matches!(part, "" | "." | ".."))
}

fn preferred_event_prefix(prefix: &str, snapshot_id: SnapshotId) -> String {
    format!("{prefix}/preferred/{snapshot_id}/")
}

fn preferred_event_key(prefix: &str, snapshot_id: SnapshotId, generation: u64) -> String {
    format!(
        "{}{generation:020}.preferred.pb",
        preferred_event_prefix(prefix, snapshot_id)
    )
}

fn pin_key(prefix: &str, pin_id: &str) -> String {
    format!("{prefix}/pins/{pin_id}.pin.pb")
}

fn retirement_key(prefix: &str, layout_id: LayoutId) -> String {
    format!("{prefix}/retirements/{layout_id}.retired.pb")
}

fn barrier_key(prefix: &str) -> String {
    format!("{prefix}/coordination/active.barrier.pb")
}

fn now_timestamp() -> prost_types::Timestamp {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(elapsed.subsec_nanos()).expect("nanoseconds fit i32"),
    }
}

#[cfg(test)]
mod tests {
    use shardpack_manifest::{layout_id, proto};

    use super::{Catalog, Error, now_timestamp};

    fn layout(snapshot: u8, builder: &str) -> proto::LayoutManifest {
        let mut value = proto::LayoutManifest {
            snapshot_id: vec![snapshot; 32],
            builder_version: builder.into(),
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
                accumulator_a: vec![0; 32],
                accumulator_b: vec![0; 32],
            }),
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        value.layout_id = layout_id(&value).0.to_vec();
        value
    }

    #[tokio::test]
    async fn persists_preferred_generations_and_layout_pins() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let catalog = Catalog::new(store, "control").unwrap();
        let first = layout(7, "first");
        let second = layout(7, "second");

        let event1 = catalog.switch_preferred(&first).await.unwrap();
        assert_eq!(event1.generation, 1);
        assert_eq!(
            catalog.switch_preferred(&first).await.unwrap().generation,
            1
        );
        let event2 = catalog.switch_preferred(&second).await.unwrap();
        assert_eq!(event2.generation, 2);
        assert_eq!(event2.previous_layout_id, Some(first.layout_id.clone()));
        let third = layout(7, "third");
        let stale = catalog
            .compare_and_switch_preferred(
                &third,
                Some(shardpack_manifest::LayoutId::from_slice(&first.layout_id).unwrap()),
            )
            .await;
        assert!(matches!(stale, Err(Error::Conflict)));
        assert_eq!(
            catalog
                .preferred(shardpack_manifest::SnapshotId([7; 32]))
                .await
                .unwrap()
                .unwrap()
                .layout_id,
            second.layout_id
        );
        let retirement = catalog
            .retire(
                &first,
                shardpack_manifest::LayoutId::from_slice(&second.layout_id).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retirement.layout_id, first.layout_id);
        assert_eq!(catalog.retirements().await.unwrap(), vec![retirement]);

        catalog
            .pin(proto::LayoutPin {
                pin_id: "job-42".into(),
                snapshot_id: second.snapshot_id.clone(),
                layout_id: second.layout_id.clone(),
                reason: proto::PinReason::RunningJob as i32,
                created_at: Some(now_timestamp()),
                expires_at: None,
            })
            .await
            .unwrap();
        assert_eq!(catalog.pins().await.unwrap().len(), 1);
        catalog.unpin("job-42").await.unwrap();
        assert!(catalog.pins().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn barrier_is_exclusive_and_requires_exact_owner_for_recovery() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let catalog = Catalog::new(store, "control").unwrap();
        let lease = catalog
            .acquire_barrier("builder-1", "publish")
            .await
            .unwrap();
        assert!(matches!(
            catalog.acquire_barrier("gc-1", "sweep").await,
            Err(Error::BarrierBusy)
        ));
        assert!(matches!(
            catalog.switch_preferred(&layout(4, "blocked")).await,
            Err(Error::BarrierBusy)
        ));
        assert!(matches!(
            catalog.recover_barrier("wrong-owner").await,
            Err(Error::BarrierNotOwned { .. })
        ));
        catalog.release_barrier(&lease).await.unwrap();
        let next = catalog.acquire_barrier("gc-1", "sweep").await.unwrap();
        catalog.recover_barrier(&next.owner_id).await.unwrap();
    }
}
