//! Auditable mark-and-sweep planning with durable last-copy protection.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use shardpack_manifest::{LayoutId, ShardId, SnapshotId, proto};
use shardpack_storage::Store;
use snafu::Snafu;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RootReason {
    Active,
    Preferred,
    RunningJob,
    ManualPin,
    Building,
    RetentionPolicy,
}

#[derive(Clone, Debug)]
pub struct SnapshotPolicy {
    pub manifest: proto::SnapshotManifest,
}

#[derive(Clone, Debug)]
pub struct LayoutRecord {
    pub manifest: proto::LayoutManifest,
    pub manifest_key: String,
    /// Mutable control-plane state, deliberately excluded from `LayoutId`.
    pub lifecycle: LayoutLifecycle,
    pub roots: BTreeSet<RootReason>,
    pub published_at_unix_seconds: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayoutLifecycle {
    Published,
    Retired,
}

#[derive(Clone, Debug)]
pub struct ShardObject {
    pub shard_id: ShardId,
    pub object_key: String,
    pub created_at_unix_seconds: i64,
}

#[derive(Clone, Debug)]
pub struct GcInput {
    pub snapshots: Vec<SnapshotPolicy>,
    pub layouts: Vec<LayoutRecord>,
    pub shard_objects: Vec<ShardObject>,
    /// Current preferred layout IDs resolved from catalog event streams.
    pub preferred_layouts: Vec<LayoutId>,
    /// Persistent job/manual/retention pins loaded from the catalog.
    pub pins: Vec<proto::LayoutPin>,
    /// Immutable retirement records loaded from the catalog.
    pub retirements: Vec<proto::LayoutRetirement>,
    pub now_unix_seconds: i64,
    pub grace_period_seconds: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcPlan {
    pub retained_layouts: BTreeMap<LayoutId, BTreeSet<RetentionReason>>,
    pub delete_layout_manifests: Vec<String>,
    pub delete_shards: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RetentionReason {
    ExplicitRoot(RootReason),
    DurableLastCopy,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("invalid snapshot manifest: {source}"))]
    SnapshotManifest { source: shardpack_manifest::Error },
    #[snafu(display("invalid complete layout manifest: {source}"))]
    LayoutManifest { source: shardpack_manifest::Error },
    #[snafu(display("layout or shard has an invalid 256-bit ID: {source}"))]
    InvalidId { source: shardpack_manifest::Error },
    #[snafu(display("durable self-contained snapshot {snapshot_id} has no complete layout"))]
    MissingDurableCopy { snapshot_id: SnapshotId },
    #[snafu(display("catalog references unknown layout {layout_id}"))]
    UnknownCatalogLayout { layout_id: LayoutId },
    #[snafu(display("catalog pin is invalid: {reason}"))]
    InvalidPin { reason: &'static str },
    #[snafu(display("catalog retirement is invalid: {reason}"))]
    InvalidRetirement { reason: &'static str },
    #[snafu(display("GC storage deletion failed for {key:?}: {source}"))]
    Delete {
        key: String,
        source: shardpack_storage::Error,
    },
    #[snafu(display("inventory storage operation failed for {key:?}: {source}"))]
    InventoryStorage {
        key: String,
        source: shardpack_storage::Error,
    },
    #[snafu(display("catalog operation failed: {source}"))]
    Catalog { source: shardpack_catalog::Error },
    #[snafu(display("invalid inventory object {key:?}: {reason}"))]
    InvalidInventoryObject { key: String, reason: &'static str },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

const MAX_INVENTORY_MANIFEST_BYTES: usize = 512 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct InventoryOptions {
    pub snapshot_prefix: String,
    pub layout_prefix: String,
    pub shard_prefix: String,
    pub now_unix_seconds: i64,
    pub grace_period_seconds: u64,
}

/// Discovers authoritative manifests, catalog roots and content-addressed
/// shard objects directly from the shared object store.
pub async fn inventory(
    store: &Store,
    catalog: &shardpack_catalog::Catalog,
    options: &InventoryOptions,
) -> Result<GcInput> {
    use prost::Message;

    let retirements = catalog
        .retirements()
        .await
        .map_err(|source| Error::Catalog { source })?;
    let retired_ids = retirements
        .iter()
        .map(|value| {
            LayoutId::from_slice(&value.layout_id).map_err(|source| Error::InvalidId { source })
        })
        .collect::<Result<HashSet<_>>>()?;
    let pins = catalog
        .pins()
        .await
        .map_err(|source| Error::Catalog { source })?;

    let mut snapshots = Vec::new();
    let mut preferred_layouts = Vec::new();
    for object in store
        .list_objects(&format!("{}/", options.snapshot_prefix))
        .await
        .map_err(|source| Error::InventoryStorage {
            key: options.snapshot_prefix.clone(),
            source,
        })?
    {
        if !object.key.ends_with(".snapshot.pb") {
            continue;
        }
        let bytes = store
            .read_all_limited(&object.key, MAX_INVENTORY_MANIFEST_BYTES)
            .await
            .map_err(|source| Error::InventoryStorage {
                key: object.key.clone(),
                source,
            })?;
        let manifest =
            proto::SnapshotManifest::decode(bytes).map_err(|source| Error::SnapshotManifest {
                source: shardpack_manifest::Error::Decode { source },
            })?;
        shardpack_manifest::validate_snapshot(&manifest)
            .map_err(|source| Error::SnapshotManifest { source })?;
        let snapshot_id = SnapshotId::from_slice(&manifest.snapshot_id)
            .map_err(|source| Error::InvalidId { source })?;
        if let Some(preferred) = catalog
            .preferred(snapshot_id)
            .await
            .map_err(|source| Error::Catalog { source })?
        {
            preferred_layouts.push(
                LayoutId::from_slice(&preferred.layout_id)
                    .map_err(|source| Error::InvalidId { source })?,
            );
        }
        snapshots.push(SnapshotPolicy { manifest });
    }

    let mut layouts = Vec::new();
    for object in store
        .list_objects(&format!("{}/", options.layout_prefix))
        .await
        .map_err(|source| Error::InventoryStorage {
            key: options.layout_prefix.clone(),
            source,
        })?
    {
        if !object.key.ends_with(".layout.pb") {
            continue;
        }
        let bytes = store
            .read_all_limited(&object.key, MAX_INVENTORY_MANIFEST_BYTES)
            .await
            .map_err(|source| Error::InventoryStorage {
                key: object.key.clone(),
                source,
            })?;
        let manifest =
            proto::LayoutManifest::decode(bytes).map_err(|source| Error::LayoutManifest {
                source: shardpack_manifest::Error::Decode { source },
            })?;
        shardpack_manifest::validate_layout(&manifest)
            .map_err(|source| Error::LayoutManifest { source })?;
        let id = LayoutId::from_slice(&manifest.layout_id)
            .map_err(|source| Error::InvalidId { source })?;
        let published_at_unix_seconds = manifest
            .created_at
            .as_ref()
            .map(|value| value.seconds)
            .or(object.last_modified_unix_seconds)
            .unwrap_or(options.now_unix_seconds);
        layouts.push(LayoutRecord {
            manifest,
            manifest_key: object.key,
            lifecycle: if retired_ids.contains(&id) {
                LayoutLifecycle::Retired
            } else {
                LayoutLifecycle::Published
            },
            roots: BTreeSet::new(),
            published_at_unix_seconds,
        });
    }

    let mut shard_objects = Vec::new();
    for object in store
        .list_objects(&format!("{}/", options.shard_prefix))
        .await
        .map_err(|source| Error::InventoryStorage {
            key: options.shard_prefix.clone(),
            source,
        })?
    {
        if !object.key.ends_with(".spk") {
            continue;
        }
        let shard_id = shard_id_from_key(&object.key)?;
        shard_objects.push(ShardObject {
            shard_id,
            object_key: object.key,
            created_at_unix_seconds: object
                .last_modified_unix_seconds
                .unwrap_or(options.now_unix_seconds),
        });
    }

    Ok(GcInput {
        snapshots,
        layouts,
        shard_objects,
        preferred_layouts,
        pins,
        retirements,
        now_unix_seconds: options.now_unix_seconds,
        grace_period_seconds: options.grace_period_seconds,
    })
}

/// Runs inventory, mark-and-sweep planning and deletion while holding the
/// shared publication/sweep barrier.
pub async fn sweep(
    store: &Store,
    catalog: &shardpack_catalog::Catalog,
    options: &InventoryOptions,
    owner_id: &str,
) -> Result<GcPlan> {
    let lease = catalog
        .acquire_barrier(owner_id, "gc-sweep")
        .await
        .map_err(|source| Error::Catalog { source })?;
    let operation = async {
        let input = inventory(store, catalog, options).await?;
        let plan = plan(&input)?;
        execute(store, &plan).await?;
        Ok(plan)
    }
    .await;
    let release = catalog
        .release_barrier(&lease)
        .await
        .map_err(|source| Error::Catalog { source });
    match operation {
        Err(error) => {
            let _ = release;
            Err(error)
        }
        Ok(plan) => {
            release?;
            Ok(plan)
        }
    }
}

fn shard_id_from_key(key: &str) -> Result<ShardId> {
    let name = key
        .rsplit('/')
        .next()
        .and_then(|value| value.strip_suffix(".spk"))
        .ok_or_else(|| Error::InvalidInventoryObject {
            key: key.to_owned(),
            reason: "expected a .spk object name",
        })?;
    let decoded = hex::decode(name).map_err(|_| Error::InvalidInventoryObject {
        key: key.to_owned(),
        reason: "shard filename must be a hexadecimal ShardId",
    })?;
    ShardId::from_slice(&decoded).map_err(|source| Error::InvalidId { source })
}

pub fn plan(input: &GcInput) -> Result<GcPlan> {
    let mut policies = HashMap::new();
    for snapshot in &input.snapshots {
        shardpack_manifest::validate_snapshot(&snapshot.manifest)
            .map_err(|source| Error::SnapshotManifest { source })?;
        let id = SnapshotId::from_slice(&snapshot.manifest.snapshot_id)
            .map_err(|source| Error::InvalidId { source })?;
        policies.insert(id, snapshot.manifest.clone());
    }

    let mut retirement_times = HashMap::new();
    let mut retirement_replacements = HashMap::new();
    let mut retirement_snapshots = HashMap::new();
    for retirement in &input.retirements {
        let layout_id = LayoutId::from_slice(&retirement.layout_id)
            .map_err(|source| Error::InvalidId { source })?;
        let replacement = LayoutId::from_slice(&retirement.replacement_layout_id)
            .map_err(|source| Error::InvalidId { source })?;
        let snapshot_id = SnapshotId::from_slice(&retirement.snapshot_id)
            .map_err(|source| Error::InvalidId { source })?;
        let retired_at = retirement
            .retired_at
            .as_ref()
            .ok_or(Error::InvalidRetirement {
                reason: "retirement timestamp is missing",
            })?;
        if layout_id == replacement
            || retirement_times
                .insert(layout_id, retired_at.seconds)
                .is_some()
        {
            return Err(Error::InvalidRetirement {
                reason: "layouts must differ and each source may be retired once",
            });
        }
        retirement_replacements.insert(layout_id, replacement);
        retirement_snapshots.insert(layout_id, snapshot_id);
    }

    let mut layout_ids = Vec::with_capacity(input.layouts.len());
    let mut layout_snapshots = HashMap::new();
    let mut complete_by_snapshot: HashMap<SnapshotId, Vec<usize>> = HashMap::new();
    let preferred: HashSet<_> = input.preferred_layouts.iter().copied().collect();
    let mut plan = GcPlan::default();
    for (index, layout) in input.layouts.iter().enumerate() {
        shardpack_manifest::validate_layout(&layout.manifest)
            .map_err(|source| Error::LayoutManifest { source })?;
        let layout_id = LayoutId::from_slice(&layout.manifest.layout_id)
            .map_err(|source| Error::InvalidId { source })?;
        let snapshot_id = SnapshotId::from_slice(&layout.manifest.snapshot_id)
            .map_err(|source| Error::InvalidId { source })?;
        layout_ids.push(layout_id);
        layout_snapshots.insert(layout_id, snapshot_id);
        if proto::LayoutState::try_from(layout.manifest.state) == Ok(proto::LayoutState::Complete) {
            complete_by_snapshot
                .entry(snapshot_id)
                .or_default()
                .push(index);
        }
        for reason in &layout.roots {
            plan.retained_layouts
                .entry(layout_id)
                .or_default()
                .insert(RetentionReason::ExplicitRoot(*reason));
        }
        if layout.lifecycle == LayoutLifecycle::Published
            && !retirement_times.contains_key(&layout_id)
        {
            plan.retained_layouts
                .entry(layout_id)
                .or_default()
                .insert(RetentionReason::ExplicitRoot(RootReason::Active));
        }
        if preferred.contains(&layout_id) {
            plan.retained_layouts
                .entry(layout_id)
                .or_default()
                .insert(RetentionReason::ExplicitRoot(RootReason::Preferred));
        }
    }

    let known_layouts: HashSet<_> = layout_ids.iter().copied().collect();
    for (source, replacement) in &retirement_replacements {
        if !known_layouts.contains(source) || !known_layouts.contains(replacement) {
            return Err(Error::UnknownCatalogLayout {
                layout_id: if known_layouts.contains(source) {
                    *replacement
                } else {
                    *source
                },
            });
        }
        if layout_snapshots[source] != layout_snapshots[replacement]
            || layout_snapshots[source] != retirement_snapshots[source]
        {
            return Err(Error::InvalidRetirement {
                reason: "source and replacement belong to different snapshots",
            });
        }
    }
    for layout_id in &preferred {
        if !known_layouts.contains(layout_id) {
            return Err(Error::UnknownCatalogLayout {
                layout_id: *layout_id,
            });
        }
    }
    for pin in &input.pins {
        let layout_id = validate_active_pin(pin, input.now_unix_seconds)?;
        let Some(layout_id) = layout_id else { continue };
        if !known_layouts.contains(&layout_id) {
            return Err(Error::UnknownCatalogLayout { layout_id });
        }
        let reason = match proto::PinReason::try_from(pin.reason) {
            Ok(proto::PinReason::RunningJob) => RootReason::RunningJob,
            Ok(proto::PinReason::Manual) => RootReason::ManualPin,
            Ok(proto::PinReason::Retention) => RootReason::RetentionPolicy,
            _ => {
                return Err(Error::InvalidPin {
                    reason: "reason must be explicit and recognized",
                });
            }
        };
        plan.retained_layouts
            .entry(layout_id)
            .or_default()
            .insert(RetentionReason::ExplicitRoot(reason));
    }

    for (snapshot_id, policy) in &policies {
        let durable = policy.durability == proto::Durability::Durable as i32;
        let portable = policy.portability == proto::Portability::SelfContained as i32;
        if !durable || !portable {
            continue;
        }
        let candidates =
            complete_by_snapshot
                .get(snapshot_id)
                .ok_or(Error::MissingDurableCopy {
                    snapshot_id: *snapshot_id,
                })?;
        let already_retained = candidates
            .iter()
            .any(|index| plan.retained_layouts.contains_key(&layout_ids[*index]));
        if !already_retained {
            let newest = candidates
                .iter()
                .copied()
                .max_by_key(|index| input.layouts[*index].published_at_unix_seconds)
                .expect("candidate list is non-empty");
            plan.retained_layouts
                .entry(layout_ids[newest])
                .or_default()
                .insert(RetentionReason::DurableLastCopy);
        }
    }

    let mut marked_shards = HashSet::new();
    for (index, layout) in input.layouts.iter().enumerate() {
        let retained = plan.retained_layouts.contains_key(&layout_ids[index]);
        let past_grace = older_than_grace(
            retirement_times
                .get(&layout_ids[index])
                .copied()
                .unwrap_or(layout.published_at_unix_seconds),
            input,
        );
        if retained || !past_grace {
            for shard in &layout.manifest.shards {
                marked_shards.insert(
                    ShardId::from_slice(&shard.shard_id)
                        .map_err(|source| Error::InvalidId { source })?,
                );
            }
        } else {
            plan.delete_layout_manifests
                .push(layout.manifest_key.clone());
        }
    }
    for object in &input.shard_objects {
        if !marked_shards.contains(&object.shard_id)
            && older_than_grace(object.created_at_unix_seconds, input)
        {
            plan.delete_shards.push(object.object_key.clone());
        }
    }
    plan.delete_layout_manifests.sort();
    plan.delete_shards.sort();
    Ok(plan)
}

pub async fn execute(store: &Store, plan: &GcPlan) -> Result<()> {
    for key in plan
        .delete_layout_manifests
        .iter()
        .chain(&plan.delete_shards)
    {
        store.delete(key).await.map_err(|source| Error::Delete {
            key: key.clone(),
            source,
        })?;
    }
    Ok(())
}

fn older_than_grace(created: i64, input: &GcInput) -> bool {
    input.now_unix_seconds.saturating_sub(created)
        >= i64::try_from(input.grace_period_seconds).unwrap_or(i64::MAX)
}

fn validate_active_pin(pin: &proto::LayoutPin, now: i64) -> Result<Option<LayoutId>> {
    if pin.pin_id.is_empty() || pin.created_at.is_none() {
        return Err(Error::InvalidPin {
            reason: "ID and creation timestamp are required",
        });
    }
    SnapshotId::from_slice(&pin.snapshot_id).map_err(|source| Error::InvalidId { source })?;
    let layout_id =
        LayoutId::from_slice(&pin.layout_id).map_err(|source| Error::InvalidId { source })?;
    if pin
        .expires_at
        .as_ref()
        .is_some_and(|expires| expires.seconds <= now)
    {
        Ok(None)
    } else {
        Ok(Some(layout_id))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::stream;
    use prost::Message;
    use shardpack_manifest::{layout_id, proto, snapshot_id};
    use std::collections::BTreeSet;

    use super::{
        Error, GcInput, InventoryOptions, LayoutLifecycle, LayoutRecord, RetentionReason,
        ShardObject, SnapshotPolicy, inventory, plan, sweep,
    };

    fn snapshot() -> proto::SnapshotManifest {
        let mut value = proto::SnapshotManifest {
            dataset_id: "durable".into(),
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
            logical_record_count: 1,
            ..Default::default()
        };
        value.snapshot_id = snapshot_id(&value).0.to_vec();
        value
    }

    fn layout(snapshot: &proto::SnapshotManifest, shard_byte: u8, created: i64) -> LayoutRecord {
        let fingerprint = proto::ContentFingerprint {
            record_count: 1,
            accumulator_a: vec![shard_byte; 32],
            accumulator_b: vec![shard_byte; 32],
        };
        let mut value = proto::LayoutManifest {
            snapshot_id: snapshot.snapshot_id.clone(),
            builder_version: "test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1,
                target_block_bytes: 1,
                block_codec: "none".into(),
                shuffle_seed: None,
            }),
            portability: snapshot.portability,
            durability: snapshot.durability,
            total_records: 1,
            total_bytes: 1,
            content_fingerprint: Some(fingerprint.clone()),
            shards: vec![proto::ShardMeta {
                shard_id: vec![shard_byte; 32],
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: format!("shards/{shard_byte}.spk"),
                }),
                byte_size: 1,
                record_count: 1,
                block_count: 1,
                content_fingerprint: Some(fingerprint),
            }],
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        value.layout_id = layout_id(&value).0.to_vec();
        LayoutRecord {
            manifest_key: format!("layouts/{shard_byte}.pb"),
            manifest: value,
            lifecycle: LayoutLifecycle::Retired,
            roots: BTreeSet::new(),
            published_at_unix_seconds: created,
        }
    }

    #[test]
    fn preserves_newest_last_durable_copy_and_sweeps_old_layout() {
        let snapshot = snapshot();
        let old = layout(&snapshot, 2, 10);
        let newest = layout(&snapshot, 3, 20);
        let input = GcInput {
            snapshots: vec![SnapshotPolicy { manifest: snapshot }],
            layouts: vec![old, newest.clone()],
            shard_objects: vec![
                ShardObject {
                    shard_id: shardpack_manifest::ShardId([2; 32]),
                    object_key: "shards/2.spk".into(),
                    created_at_unix_seconds: 10,
                },
                ShardObject {
                    shard_id: shardpack_manifest::ShardId([3; 32]),
                    object_key: "shards/3.spk".into(),
                    created_at_unix_seconds: 20,
                },
            ],
            preferred_layouts: Vec::new(),
            pins: Vec::new(),
            retirements: Vec::new(),
            now_unix_seconds: 100,
            grace_period_seconds: 10,
        };
        let result = plan(&input).unwrap();
        let newest_id =
            shardpack_manifest::LayoutId::from_slice(&newest.manifest.layout_id).unwrap();
        assert!(result.retained_layouts[&newest_id].contains(&RetentionReason::DurableLastCopy));
        assert_eq!(result.delete_layout_manifests, vec!["layouts/2.pb"]);
        assert_eq!(result.delete_shards, vec!["shards/2.spk"]);
    }

    #[test]
    fn incomplete_layout_never_satisfies_durable_last_copy() {
        let snapshot = snapshot();
        let mut incomplete = layout(&snapshot, 2, 10);
        incomplete.manifest.state = proto::LayoutState::Incomplete as i32;
        incomplete.manifest.materialization_stats = Some(proto::MaterializationStats {
            expected_records: 2,
            written_records: 1,
            skipped_records: 1,
        });
        incomplete.manifest.layout_id = layout_id(&incomplete.manifest).0.to_vec();
        let result = plan(&GcInput {
            snapshots: vec![SnapshotPolicy { manifest: snapshot }],
            layouts: vec![incomplete],
            shard_objects: Vec::new(),
            preferred_layouts: Vec::new(),
            pins: Vec::new(),
            retirements: Vec::new(),
            now_unix_seconds: 100,
            grace_period_seconds: 10,
        });
        assert!(matches!(result, Err(Error::MissingDurableCopy { .. })));
    }

    #[test]
    fn last_copy_protection_applies_only_to_durable_portable_snapshots() {
        for portability in [
            proto::Portability::SelfContained,
            proto::Portability::ExternalReferences,
        ] {
            for durability in [proto::Durability::Durable, proto::Durability::Rebuildable] {
                let mut snapshot = snapshot();
                snapshot.portability = portability as i32;
                snapshot.durability = durability as i32;
                snapshot.snapshot_id = snapshot_id(&snapshot).0.to_vec();
                let result = plan(&GcInput {
                    snapshots: vec![SnapshotPolicy { manifest: snapshot }],
                    layouts: Vec::new(),
                    shard_objects: Vec::new(),
                    preferred_layouts: Vec::new(),
                    pins: Vec::new(),
                    retirements: Vec::new(),
                    now_unix_seconds: 100,
                    grace_period_seconds: 10,
                });
                let protected = portability == proto::Portability::SelfContained
                    && durability == proto::Durability::Durable;
                assert_eq!(
                    matches!(result, Err(Error::MissingDurableCopy { .. })),
                    protected,
                    "unexpected last-copy policy for {portability:?}/{durability:?}"
                );
            }
        }
    }

    #[test]
    fn persistent_job_pin_keeps_a_retired_layout_and_its_shards() {
        let snapshot = snapshot();
        let old = layout(&snapshot, 2, 10);
        let newest = layout(&snapshot, 3, 20);
        let old_id = old.manifest.layout_id.clone();
        let input = GcInput {
            snapshots: vec![SnapshotPolicy {
                manifest: snapshot.clone(),
            }],
            layouts: vec![old, newest],
            shard_objects: vec![ShardObject {
                shard_id: shardpack_manifest::ShardId([2; 32]),
                object_key: "shards/2.spk".into(),
                created_at_unix_seconds: 10,
            }],
            preferred_layouts: Vec::new(),
            pins: vec![proto::LayoutPin {
                pin_id: "training-job".into(),
                snapshot_id: snapshot.snapshot_id,
                layout_id: old_id.clone(),
                reason: proto::PinReason::RunningJob as i32,
                created_at: Some(prost_types::Timestamp {
                    seconds: 10,
                    nanos: 0,
                }),
                expires_at: None,
            }],
            retirements: Vec::new(),
            now_unix_seconds: 100,
            grace_period_seconds: 10,
        };
        let result = plan(&input).unwrap();
        let old_id = shardpack_manifest::LayoutId::from_slice(&old_id).unwrap();
        assert!(
            result.retained_layouts[&old_id].contains(&RetentionReason::ExplicitRoot(
                super::RootReason::RunningJob
            ))
        );
        assert!(result.delete_shards.is_empty());
    }

    #[test]
    fn expired_pin_does_not_keep_an_old_layout_or_its_shard() {
        let snapshot = snapshot();
        let old = layout(&snapshot, 2, 10);
        let newest = layout(&snapshot, 3, 20);
        let old_id = old.manifest.layout_id.clone();
        let result = plan(&GcInput {
            snapshots: vec![SnapshotPolicy {
                manifest: snapshot.clone(),
            }],
            layouts: vec![old, newest],
            shard_objects: vec![ShardObject {
                shard_id: shardpack_manifest::ShardId([2; 32]),
                object_key: "shards/2.spk".into(),
                created_at_unix_seconds: 10,
            }],
            preferred_layouts: Vec::new(),
            pins: vec![proto::LayoutPin {
                pin_id: "expired-job".into(),
                snapshot_id: snapshot.snapshot_id,
                layout_id: old_id,
                reason: proto::PinReason::RunningJob as i32,
                created_at: Some(prost_types::Timestamp {
                    seconds: 10,
                    nanos: 0,
                }),
                expires_at: Some(prost_types::Timestamp {
                    seconds: 99,
                    nanos: 0,
                }),
            }],
            retirements: Vec::new(),
            now_unix_seconds: 100,
            grace_period_seconds: 10,
        })
        .unwrap();
        assert_eq!(result.delete_layout_manifests, vec!["layouts/2.pb"]);
        assert_eq!(result.delete_shards, vec!["shards/2.spk"]);
    }

    #[test]
    fn retirement_grace_protects_layout_and_shards_from_sweep() {
        let snapshot = snapshot();
        let old = layout(&snapshot, 2, 1);
        let newest = layout(&snapshot, 3, 2);
        let old_id = old.manifest.layout_id.clone();
        let newest_id = newest.manifest.layout_id.clone();
        let input = GcInput {
            snapshots: vec![SnapshotPolicy {
                manifest: snapshot.clone(),
            }],
            layouts: vec![old, newest],
            shard_objects: vec![ShardObject {
                shard_id: shardpack_manifest::ShardId([2; 32]),
                object_key: "shards/2.spk".into(),
                created_at_unix_seconds: 1,
            }],
            preferred_layouts: vec![shardpack_manifest::LayoutId::from_slice(&newest_id).unwrap()],
            pins: Vec::new(),
            retirements: vec![proto::LayoutRetirement {
                snapshot_id: snapshot.snapshot_id,
                layout_id: old_id,
                replacement_layout_id: newest_id,
                retired_at: Some(prost_types::Timestamp {
                    seconds: 95,
                    nanos: 0,
                }),
            }],
            now_unix_seconds: 100,
            grace_period_seconds: 10,
        };
        let result = plan(&input).unwrap();
        assert!(result.delete_layout_manifests.is_empty());
        assert!(result.delete_shards.is_empty());
    }

    #[tokio::test]
    async fn inventory_discovers_referenced_and_orphan_shards() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let snapshot = snapshot();
        let mut layout = layout(&snapshot, 2, 1).manifest;
        let referenced_key = format!("shards/{}.spk", hex::encode([2; 32]));
        layout.shards[0]
            .location
            .as_mut()
            .unwrap()
            .object_key
            .clone_from(&referenced_key);
        layout.layout_id = layout_id(&layout).0.to_vec();
        let orphan_key = format!("shards/{}.spk", hex::encode([9; 32]));

        for (key, bytes) in [
            ("snapshots/test.snapshot.pb", snapshot.encode_to_vec()),
            ("layouts/test.layout.pb", layout.encode_to_vec()),
            (&referenced_key, vec![2]),
            (&orphan_key, vec![9]),
        ] {
            store
                .write_immutable(
                    key,
                    Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) })),
                )
                .await
                .unwrap();
        }
        let catalog = shardpack_catalog::Catalog::new(store.clone(), "control").unwrap();
        catalog.switch_preferred(&layout).await.unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 100;
        let input = inventory(
            &store,
            &catalog,
            &InventoryOptions {
                snapshot_prefix: "snapshots".into(),
                layout_prefix: "layouts".into(),
                shard_prefix: "shards".into(),
                now_unix_seconds: now,
                grace_period_seconds: 10,
            },
        )
        .await
        .unwrap();
        let result = plan(&input).unwrap();
        assert_eq!(result.delete_shards, vec![orphan_key.clone()]);
        assert!(result.delete_layout_manifests.is_empty());

        let executed = sweep(
            &store,
            &catalog,
            &InventoryOptions {
                snapshot_prefix: "snapshots".into(),
                layout_prefix: "layouts".into(),
                shard_prefix: "shards".into(),
                now_unix_seconds: now,
                grace_period_seconds: 10,
            },
            "gc-test",
        )
        .await
        .unwrap();
        assert_eq!(executed.delete_shards, vec![orphan_key.clone()]);
        assert!(store.operator().stat(&orphan_key).await.is_err());
        let lease = catalog
            .acquire_barrier("after-gc", "verification")
            .await
            .unwrap();
        catalog.release_barrier(&lease).await.unwrap();
    }
}
