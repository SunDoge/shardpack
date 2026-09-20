//! Authoritative protobuf manifests and deterministic immutable IDs.

use prost::Message;
use snafu::Snafu;
use std::collections::HashSet;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/shardpack.v1.rs"));
}

/// Returns a shard's physical location after its containing layout has been
/// validated.
#[must_use]
pub fn object_location(shard: &proto::ShardMeta) -> &proto::ObjectLocation {
    shard
        .location
        .as_ref()
        .expect("validated shard location must be present")
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("protobuf decode failed: {source}"))]
    Decode { source: prost::DecodeError },
    #[snafu(display("invalid {field}: {reason}"))]
    Invalid {
        field: &'static str,
        reason: &'static str,
    },
    #[snafu(display("manifest ID does not match its canonical content"))]
    IdentityMismatch,
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! hash_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub [u8; 32]);
        impl $name {
            pub fn from_slice(value: &[u8]) -> Result<Self> {
                let bytes = value.try_into().map_err(|_| Error::Invalid {
                    field: stringify!($name),
                    reason: "must be 32 bytes",
                })?;
                Ok(Self(bytes))
            }
            #[must_use]
            pub fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&hex::encode(self.0))
            }
        }
    };
}
hash_id!(SnapshotId);
hash_id!(LayoutId);
hash_id!(ShardId);

pub fn snapshot_id(manifest: &proto::SnapshotManifest) -> SnapshotId {
    let mut canonical = manifest.clone();
    canonical.snapshot_id.clear();
    canonical.created_at = None;
    SnapshotId(domain_hash(
        b"shardpack-snapshot-v1",
        &canonical.encode_to_vec(),
    ))
}

pub fn layout_id(manifest: &proto::LayoutManifest) -> LayoutId {
    let mut canonical = manifest.clone();
    canonical.layout_id.clear();
    canonical.created_at = None;
    LayoutId(domain_hash(
        b"shardpack-layout-v1",
        &canonical.encode_to_vec(),
    ))
}

pub fn validate_snapshot(manifest: &proto::SnapshotManifest) -> Result<()> {
    validate_hash("snapshot_id", &manifest.snapshot_id)?;
    if manifest.dataset_id.is_empty() {
        return Err(Error::Invalid {
            field: "dataset_id",
            reason: "must not be empty",
        });
    }
    if manifest
        .source
        .as_ref()
        .and_then(|value| value.source.as_ref())
        .is_none()
    {
        return Err(Error::Invalid {
            field: "source",
            reason: "must be set",
        });
    }
    let schema = manifest.record_schema.as_ref().ok_or(Error::Invalid {
        field: "record_schema",
        reason: "must be set",
    })?;
    if schema.codec_name.is_empty() {
        return Err(Error::Invalid {
            field: "codec_name",
            reason: "must not be empty",
        });
    }
    validate_hash("schema_id", &schema.schema_id)?;
    if let Some(parent) = &manifest.parent_snapshot_id {
        validate_hash("parent_snapshot_id", parent)?;
    }
    let mut split_names = HashSet::new();
    let mut split_records = 0_u64;
    let mut previous_bound = 0_u32;
    for split in &manifest.splits {
        if split.name.is_empty() || !split_names.insert(split.name.as_str()) {
            return Err(Error::Invalid {
                field: "splits",
                reason: "names must be non-empty and unique",
            });
        }
        split_records = split_records
            .checked_add(split.record_count)
            .ok_or(Error::Invalid {
                field: "splits",
                reason: "record count overflow",
            })?;
        let bound = split.hash_upper_bound.ok_or(Error::Invalid {
            field: "splits.hash_upper_bound",
            reason: "must be set for every split",
        })?;
        if bound <= previous_bound {
            return Err(Error::Invalid {
                field: "splits.hash_upper_bound",
                reason: "must be strictly increasing",
            });
        }
        previous_bound = bound;
    }
    if !manifest.splits.is_empty() && split_records != manifest.logical_record_count {
        return Err(Error::Invalid {
            field: "splits",
            reason: "record counts do not equal logical record count",
        });
    }
    match (&manifest.split_assignment, manifest.splits.is_empty()) {
        (None, false) => {
            return Err(Error::Invalid {
                field: "split_assignment",
                reason: "must be set when splits are present",
            });
        }
        (Some(_), true) => {
            return Err(Error::Invalid {
                field: "split_assignment",
                reason: "must be absent when there are no splits",
            });
        }
        (Some(assignment), false) => {
            if assignment.modulus == 0 || previous_bound != assignment.modulus {
                return Err(Error::Invalid {
                    field: "split_assignment.modulus",
                    reason: "must be nonzero and equal the final split bound",
                });
            }
            if assignment
                .group_field
                .as_ref()
                .is_some_and(String::is_empty)
            {
                return Err(Error::Invalid {
                    field: "split_assignment.group_field",
                    reason: "must be non-empty when set",
                });
            }
        }
        (None, true) => {}
    }
    if proto::Portability::try_from(manifest.portability)
        .ok()
        .is_none_or(|value| value == proto::Portability::Unspecified)
    {
        return Err(Error::Invalid {
            field: "portability",
            reason: "must be explicit",
        });
    }
    if proto::Durability::try_from(manifest.durability)
        .ok()
        .is_none_or(|value| value == proto::Durability::Unspecified)
    {
        return Err(Error::Invalid {
            field: "durability",
            reason: "must be explicit",
        });
    }
    if let Some(stats) = &manifest.materialization_stats {
        validate_materialization_stats(stats, manifest.logical_record_count)?;
    }
    if manifest.snapshot_id != snapshot_id(manifest).0 {
        return Err(Error::IdentityMismatch);
    }
    Ok(())
}

pub fn validate_layout(manifest: &proto::LayoutManifest) -> Result<()> {
    validate_hash("layout_id", &manifest.layout_id)?;
    validate_hash("snapshot_id", &manifest.snapshot_id)?;
    if let Some(encryption) = &manifest.encryption {
        if proto::EncryptionAlgorithm::try_from(encryption.algorithm)
            != Ok(proto::EncryptionAlgorithm::Aes256Gcm)
        {
            return Err(Error::Invalid {
                field: "encryption.algorithm",
                reason: "must be AES_256_GCM",
            });
        }
        if encryption.key_id.is_empty() {
            return Err(Error::Invalid {
                field: "encryption.key_id",
                reason: "must not be empty",
            });
        }
    }
    let fingerprint = manifest
        .content_fingerprint
        .as_ref()
        .ok_or(Error::Invalid {
            field: "content_fingerprint",
            reason: "must be set",
        })?;
    validate_hash("accumulator_a", &fingerprint.accumulator_a)?;
    validate_hash("accumulator_b", &fingerprint.accumulator_b)?;
    if manifest.builder_version.is_empty() {
        return Err(Error::Invalid {
            field: "builder_version",
            reason: "must not be empty",
        });
    }
    if manifest.split_name.as_ref().is_some_and(String::is_empty) {
        return Err(Error::Invalid {
            field: "split_name",
            reason: "must be non-empty when set",
        });
    }
    let recipe = manifest.recipe.as_ref().ok_or(Error::Invalid {
        field: "recipe",
        reason: "must be set",
    })?;
    if recipe.target_shard_bytes == 0 || recipe.target_block_bytes == 0 {
        return Err(Error::Invalid {
            field: "recipe",
            reason: "target sizes must be greater than zero",
        });
    }
    if !matches!(recipe.block_codec.as_str(), "none" | "zstd") {
        return Err(Error::Invalid {
            field: "recipe.block_codec",
            reason: "unsupported codec",
        });
    }
    if proto::Portability::try_from(manifest.portability)
        .ok()
        .is_none_or(|value| value == proto::Portability::Unspecified)
    {
        return Err(Error::Invalid {
            field: "portability",
            reason: "must be explicit and recognized",
        });
    }
    if proto::Durability::try_from(manifest.durability)
        .ok()
        .is_none_or(|value| value == proto::Durability::Unspecified)
    {
        return Err(Error::Invalid {
            field: "durability",
            reason: "must be explicit and recognized",
        });
    }
    let state = proto::LayoutState::try_from(manifest.state).ok();
    match (state, manifest.materialization_stats.as_ref()) {
        (Some(proto::LayoutState::Complete), None) => {}
        (Some(proto::LayoutState::Complete), Some(stats)) => {
            validate_materialization_stats(stats, manifest.total_records)?;
            if stats.skipped_records != 0 {
                return Err(Error::Invalid {
                    field: "state",
                    reason: "complete layout cannot contain skipped records",
                });
            }
        }
        (Some(proto::LayoutState::Incomplete), Some(stats)) => {
            validate_materialization_stats(stats, manifest.total_records)?;
            if stats.skipped_records == 0 {
                return Err(Error::Invalid {
                    field: "state",
                    reason: "incomplete layout must contain skipped records",
                });
            }
        }
        (Some(proto::LayoutState::Incomplete), None) => {
            return Err(Error::Invalid {
                field: "materialization_stats",
                reason: "incomplete layout requires explicit counts",
            });
        }
        _ => {
            return Err(Error::Invalid {
                field: "state",
                reason: "published layout must be complete or explicitly incomplete",
            });
        }
    }
    let mut records = 0_u64;
    let mut bytes = 0_u64;
    for shard in &manifest.shards {
        records = records
            .checked_add(shard.record_count)
            .ok_or(Error::Invalid {
                field: "total_records",
                reason: "shard sum overflow",
            })?;
        bytes = bytes.checked_add(shard.byte_size).ok_or(Error::Invalid {
            field: "total_bytes",
            reason: "shard sum overflow",
        })?;
    }
    if records != manifest.total_records {
        return Err(Error::Invalid {
            field: "total_records",
            reason: "does not equal shard sum",
        });
    }
    if bytes != manifest.total_bytes {
        return Err(Error::Invalid {
            field: "total_bytes",
            reason: "does not equal shard sum",
        });
    }
    if fingerprint.record_count != records {
        return Err(Error::Invalid {
            field: "content_fingerprint",
            reason: "record count mismatch",
        });
    }
    let mut accumulated_a = [0_u8; 32];
    let mut accumulated_b = [0_u8; 32];
    let mut shard_ids = HashSet::new();
    let mut object_locations = HashSet::new();
    for shard in &manifest.shards {
        validate_hash("shard_id", &shard.shard_id)?;
        if !shard_ids.insert(shard.shard_id.as_slice()) {
            return Err(Error::Invalid {
                field: "shards",
                reason: "duplicate shard ID",
            });
        }
        let location = shard.location.as_ref().ok_or(Error::Invalid {
            field: "shard.location",
            reason: "must be set",
        })?;
        if location.store_id.is_empty()
            || !location
                .store_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            || location.object_key.is_empty()
            || location.object_key.starts_with('/')
            || location
                .object_key
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || !object_locations.insert((location.store_id.as_str(), location.object_key.as_str()))
        {
            return Err(Error::Invalid {
                field: "shards",
                reason: "object locations must be normalized, non-empty and unique",
            });
        }
        if shard.byte_size == 0 || shard.block_count == 0 || shard.record_count == 0 {
            return Err(Error::Invalid {
                field: "shards",
                reason: "published shards must be non-empty",
            });
        }
        let shard_fingerprint = shard.content_fingerprint.as_ref().ok_or(Error::Invalid {
            field: "shard.content_fingerprint",
            reason: "must be set",
        })?;
        validate_hash("shard.accumulator_a", &shard_fingerprint.accumulator_a)?;
        validate_hash("shard.accumulator_b", &shard_fingerprint.accumulator_b)?;
        if shard_fingerprint.record_count != shard.record_count {
            return Err(Error::Invalid {
                field: "shard.content_fingerprint",
                reason: "record count mismatch",
            });
        }
        add_256(&mut accumulated_a, &shard_fingerprint.accumulator_a);
        add_256(&mut accumulated_b, &shard_fingerprint.accumulator_b);
    }
    if fingerprint.accumulator_a != accumulated_a || fingerprint.accumulator_b != accumulated_b {
        return Err(Error::Invalid {
            field: "content_fingerprint",
            reason: "does not equal shard fingerprint sum",
        });
    }
    if manifest.layout_id != layout_id(manifest).0 {
        return Err(Error::IdentityMismatch);
    }
    Ok(())
}

fn validate_materialization_stats(
    stats: &proto::MaterializationStats,
    written_records: u64,
) -> Result<()> {
    if stats.written_records != written_records {
        return Err(Error::Invalid {
            field: "materialization_stats.written_records",
            reason: "must equal the manifest record count",
        });
    }
    if stats.written_records.checked_add(stats.skipped_records) != Some(stats.expected_records) {
        return Err(Error::Invalid {
            field: "materialization_stats",
            reason: "expected must equal written plus skipped",
        });
    }
    Ok(())
}

pub fn assign_split<'a>(
    snapshot: &'a proto::SnapshotManifest,
    sample_id: &[u8; 16],
    group_id: Option<&[u8]>,
) -> Result<&'a str> {
    validate_snapshot(snapshot)?;
    let assignment = snapshot.split_assignment.as_ref().ok_or(Error::Invalid {
        field: "split_assignment",
        reason: "snapshot has no split policy",
    })?;
    let identity = if assignment.group_field.is_some() {
        group_id.ok_or(Error::Invalid {
            field: "group_id",
            reason: "required by group-aware split policy",
        })?
    } else {
        sample_id
    };
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"shardpack-split-v1");
    hasher.update(&assignment.seed.to_le_bytes());
    hasher.update(identity);
    let digest = hasher.finalize();
    let bucket = u64::from_le_bytes(digest.as_bytes()[..8].try_into().expect("eight bytes"))
        % u64::from(assignment.modulus);
    snapshot
        .splits
        .iter()
        .find(|split| bucket < u64::from(split.hash_upper_bound.expect("validated bound")))
        .map(|split| split.name.as_str())
        .ok_or(Error::Invalid {
            field: "splits",
            reason: "bucket is not covered by split bounds",
        })
}

fn add_256(target: &mut [u8; 32], value: &[u8]) {
    let mut carry = 0_u16;
    for (left, right) in target.iter_mut().zip(value) {
        let sum = u16::from(*left) + u16::from(*right) + carry;
        *left = sum.to_le_bytes()[0];
        carry = sum >> 8;
    }
}

fn validate_hash(field: &'static str, bytes: &[u8]) -> Result<()> {
    if bytes.len() == 32 {
        Ok(())
    } else {
        Err(Error::Invalid {
            field,
            reason: "must be 32 bytes",
        })
    }
}

fn domain_hash(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::{assign_split, layout_id, proto, snapshot_id, validate_layout, validate_snapshot};

    fn fingerprint(count: u64, byte: u8) -> proto::ContentFingerprint {
        proto::ContentFingerprint {
            record_count: count,
            accumulator_a: vec![byte; 32],
            accumulator_b: vec![byte; 32],
        }
    }

    #[test]
    fn snapshot_id_ignores_creation_time() {
        let mut manifest = proto::SnapshotManifest {
            dataset_id: "images".into(),
            source: Some(proto::SnapshotSource {
                source: Some(proto::snapshot_source::Source::Imported(
                    proto::ImportedSource {
                        description: Some("fixture".into()),
                    },
                )),
            }),
            record_schema: Some(proto::RecordSchemaDescriptor {
                codec_name: "example".into(),
                codec_version: 1,
                schema_id: vec![7; 32],
                embedded_schema: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            ..Default::default()
        };
        manifest.snapshot_id = snapshot_id(&manifest).0.to_vec();
        assert!(validate_snapshot(&manifest).is_ok());
        let original = snapshot_id(&manifest);
        manifest.created_at = Some(prost_types::Timestamp {
            seconds: 42,
            nanos: 0,
        });
        assert_eq!(snapshot_id(&manifest), original);
    }

    #[test]
    fn layout_requires_valid_aggregate_fingerprint() {
        let mut manifest = proto::LayoutManifest {
            snapshot_id: vec![1; 32],
            builder_version: "test-builder".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 10,
                target_block_bytes: 5,
                block_codec: "none".into(),
                shuffle_seed: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            total_records: 2,
            total_bytes: 100,
            content_fingerprint: Some(fingerprint(2, 2)),
            shards: vec![proto::ShardMeta {
                shard_id: vec![3; 32],
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: "objects/03.spk".into(),
                }),
                byte_size: 100,
                record_count: 2,
                block_count: 1,
                content_fingerprint: Some(fingerprint(2, 2)),
            }],
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        manifest.layout_id = layout_id(&manifest).0.to_vec();
        assert!(validate_layout(&manifest).is_ok());
        manifest.content_fingerprint.as_mut().unwrap().accumulator_a[0] ^= 1;
        assert!(validate_layout(&manifest).is_err());
    }

    #[test]
    fn group_aware_split_is_stable_and_validated() {
        let mut manifest = proto::SnapshotManifest {
            dataset_id: "video".into(),
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
            splits: vec![
                proto::SplitDescriptor {
                    name: "train".into(),
                    record_count: 8,
                    assignment: "hash(scene_id, 42)".into(),
                    hash_upper_bound: Some(8_000),
                },
                proto::SplitDescriptor {
                    name: "val".into(),
                    record_count: 2,
                    assignment: "hash(scene_id, 42)".into(),
                    hash_upper_bound: Some(10_000),
                },
            ],
            split_assignment: Some(proto::SplitAssignment {
                seed: 42,
                modulus: 10_000,
                group_field: Some("scene_id".into()),
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            logical_record_count: 10,
            ..Default::default()
        };
        manifest.snapshot_id = snapshot_id(&manifest).0.to_vec();
        assert!(validate_snapshot(&manifest).is_ok());
        let first = assign_split(&manifest, &[1; 16], Some(b"scene-a")).unwrap();
        let adjacent_frame = assign_split(&manifest, &[2; 16], Some(b"scene-a")).unwrap();
        assert_eq!(first, adjacent_frame);
        assert!(assign_split(&manifest, &[1; 16], None).is_err());

        manifest.splits[1].hash_upper_bound = Some(9_999);
        manifest.snapshot_id = snapshot_id(&manifest).0.to_vec();
        assert!(validate_snapshot(&manifest).is_err());
    }
}
