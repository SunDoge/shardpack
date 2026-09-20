//! Explicit, resource-bounded training data pipeline primitives.

use bytes::Bytes;
use shardpack_manifest::ShardId;
use snafu::Snafu;

mod dataset_reader;
mod job_pin;
mod metrics;
mod pipeline;
mod prefetch;
pub use dataset_reader::{
    DatasetReader, IncompleteLayoutPolicy, LoaderCheckpoint, ReaderPosition, RetryPolicy,
};
pub use job_pin::JobPin;
pub use metrics::{RuntimeMetrics, RuntimeMetricsSnapshot};
pub use pipeline::{
    ByteSize, PipelineCheckpoint, PipelineConfig, QuiescedPipeline, RecordDecoder, RecordTransform,
    RegistryRecordDecoder, RuntimeLimits, TrainingPipeline,
};
pub use prefetch::NetworkPrefetch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DistributedContext {
    pub rank: usize,
    pub world_size: usize,
    pub worker: usize,
    pub workers_per_rank: usize,
}

/// Stable node membership plus the process/worker topology local to one node.
///
/// `node_id` must survive job restarts (for example, a Kubernetes node name or
/// cloud instance identity). `node_ids` is a set: its order has no semantic
/// meaning. Changing `epoch_seed` only changes order within a node; changing
/// membership moves only shards whose rendezvous winner changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeAffinity {
    pub node_id: String,
    pub node_ids: Vec<String>,
    pub local_rank: usize,
    pub local_world_size: usize,
    pub worker: usize,
    pub workers_per_rank: usize,
    pub affinity_seed: u64,
}

#[derive(Clone, Debug)]
pub struct DecryptionContext {
    pub key_id: String,
    pub key: shardpack_format::EncryptionKey,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("invalid distributed topology"))]
    InvalidTopology,
    #[snafu(display("invalid node affinity: {reason}"))]
    InvalidNodeAffinity { reason: &'static str },
    #[snafu(display("record of {bytes} bytes exceeds shuffle byte limit {limit}"))]
    RecordTooLarge { bytes: usize, limit: usize },
    #[snafu(display("storage operation failed: {source}"))]
    Storage { source: shardpack_storage::Error },
    #[snafu(display("cache operation failed: {source}"))]
    Cache { source: shardpack_cache::Error },
    #[snafu(display("ShardPack format operation failed: {source}"))]
    Format { source: shardpack_format::Error },
    #[snafu(display("manifest validation failed: {source}"))]
    Manifest { source: shardpack_manifest::Error },
    #[snafu(display("shard {shard} failed manifest verification: {reason}"))]
    ShardMismatch { shard: String, reason: &'static str },
    #[snafu(display("invalid loader checkpoint: {reason}"))]
    InvalidCheckpoint { reason: &'static str },
    #[snafu(display("incomplete layout requires explicit opt-in"))]
    IncompleteLayout,
    #[snafu(display("decryption key ID does not match the layout"))]
    DecryptionKeyMismatch,
    #[snafu(display("invalid runtime limit {field}: {reason}"))]
    InvalidLimit {
        field: &'static str,
        reason: &'static str,
    },
    #[snafu(display("pipeline stage {stage} failed: {message}"))]
    PipelineStage {
        stage: &'static str,
        message: String,
    },
    #[snafu(display("pipeline queue closed during {stage}"))]
    QueueClosed { stage: &'static str },
    #[snafu(display("pipeline checkpoint is unavailable because the source stage terminated"))]
    CheckpointUnavailable,
    #[snafu(display("training job pin operation failed: {source}"))]
    Catalog { source: shardpack_catalog::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn assigned_shards(
    shards: impl IntoIterator<Item = ShardId>,
    epoch_seed: u64,
    context: DistributedContext,
) -> Result<Vec<ShardId>> {
    let (consumer, consumers) = distributed_consumer(context)?;
    let mut keyed: Vec<_> = shards
        .into_iter()
        .map(|id| (shuffle_key(&id, epoch_seed), id))
        .collect();
    keyed.sort_unstable_by_key(|(key, id)| (*key, *id));
    Ok(keyed
        .into_iter()
        .enumerate()
        .filter_map(|(index, (_, id))| (index % consumers == consumer).then_some(id))
        .collect())
}

pub(crate) fn assigned_shard_indices(
    shards: impl IntoIterator<Item = (usize, ShardId)>,
    epoch_seed: u64,
    context: DistributedContext,
) -> Result<Vec<usize>> {
    let (consumer, consumers) = distributed_consumer(context)?;
    let mut keyed = shards
        .into_iter()
        .map(|(index, id)| (shuffle_key(&id, epoch_seed), id, index))
        .collect::<Vec<_>>();
    keyed.sort_unstable_by_key(|(key, id, _)| (*key, *id));
    Ok(keyed
        .into_iter()
        .enumerate()
        .filter_map(|(position, (_, _, index))| (position % consumers == consumer).then_some(index))
        .collect())
}

fn distributed_consumer(context: DistributedContext) -> Result<(usize, usize)> {
    if context.world_size == 0
        || context.workers_per_rank == 0
        || context.rank >= context.world_size
        || context.worker >= context.workers_per_rank
    {
        return Err(Error::InvalidTopology);
    }
    let consumers = context
        .world_size
        .checked_mul(context.workers_per_rank)
        .ok_or(Error::InvalidTopology)?;
    let consumer = context
        .rank
        .checked_mul(context.workers_per_rank)
        .and_then(|value| value.checked_add(context.worker))
        .ok_or(Error::InvalidTopology)?;
    Ok((consumer, consumers))
}

/// Assigns each shard to a stable node using highest-random-weight hashing,
/// then shuffles and partitions that node's shards among its local consumers.
///
/// Node ownership deliberately does not include `epoch_seed`, which preserves
/// node-local cache hits across epochs. The epoch seed still provides a fresh,
/// deterministic read order inside each node.
pub fn affinity_assigned_shards(
    shards: impl IntoIterator<Item = ShardId>,
    epoch_seed: u64,
    affinity: &NodeAffinity,
) -> Result<Vec<ShardId>> {
    validate_affinity(affinity)?;
    let mut local = shards
        .into_iter()
        .filter(|id| {
            preferred_node(id, affinity.affinity_seed, &affinity.node_ids) == affinity.node_id
        })
        .map(|id| (shuffle_key(&id, epoch_seed), id))
        .collect::<Vec<_>>();
    local.sort_unstable_by_key(|(key, id)| (*key, *id));

    let consumers = affinity
        .local_world_size
        .checked_mul(affinity.workers_per_rank)
        .ok_or(Error::InvalidNodeAffinity {
            reason: "local consumer count overflows usize",
        })?;
    let consumer = affinity.local_rank * affinity.workers_per_rank + affinity.worker;
    Ok(local
        .into_iter()
        .enumerate()
        .filter_map(|(index, (_, id))| (index % consumers == consumer).then_some(id))
        .collect())
}

pub(crate) fn affinity_assigned_shard_indices(
    shards: impl IntoIterator<Item = (usize, ShardId)>,
    epoch_seed: u64,
    affinity: &NodeAffinity,
) -> Result<Vec<usize>> {
    validate_affinity(affinity)?;
    let mut local = shards
        .into_iter()
        .filter(|(_, id)| {
            preferred_node(id, affinity.affinity_seed, &affinity.node_ids) == affinity.node_id
        })
        .map(|(index, id)| (shuffle_key(&id, epoch_seed), id, index))
        .collect::<Vec<_>>();
    local.sort_unstable_by_key(|(key, id, _)| (*key, *id));

    let consumers = affinity
        .local_world_size
        .checked_mul(affinity.workers_per_rank)
        .ok_or(Error::InvalidNodeAffinity {
            reason: "local consumer count overflows usize",
        })?;
    let consumer = affinity.local_rank * affinity.workers_per_rank + affinity.worker;
    Ok(local
        .into_iter()
        .enumerate()
        .filter_map(|(position, (_, _, index))| (position % consumers == consumer).then_some(index))
        .collect())
}

fn validate_affinity(affinity: &NodeAffinity) -> Result<()> {
    if affinity.node_id.is_empty() || affinity.node_ids.is_empty() {
        return Err(Error::InvalidNodeAffinity {
            reason: "node IDs must not be empty",
        });
    }
    if affinity.local_world_size == 0
        || affinity.workers_per_rank == 0
        || affinity.local_rank >= affinity.local_world_size
        || affinity.worker >= affinity.workers_per_rank
    {
        return Err(Error::InvalidNodeAffinity {
            reason: "invalid local rank/worker topology",
        });
    }
    let mut nodes = affinity.node_ids.iter().collect::<Vec<_>>();
    nodes.sort_unstable();
    if nodes.iter().any(|node| node.is_empty()) || nodes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(Error::InvalidNodeAffinity {
            reason: "membership contains an empty or duplicate node ID",
        });
    }
    if !affinity.node_ids.contains(&affinity.node_id) {
        return Err(Error::InvalidNodeAffinity {
            reason: "current node is absent from membership",
        });
    }
    Ok(())
}

fn preferred_node<'a>(id: &ShardId, seed: u64, nodes: &'a [String]) -> &'a str {
    nodes
        .iter()
        .max_by(|left, right| {
            affinity_score(id, seed, left)
                .cmp(&affinity_score(id, seed, right))
                .then_with(|| left.cmp(right))
        })
        .expect("validated non-empty membership")
}

fn affinity_score(id: &ShardId, seed: u64, node: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"shardpack-node-affinity-v1");
    hasher.update(&seed.to_le_bytes());
    hasher.update(id.as_bytes());
    hasher.update(&(node.len() as u64).to_le_bytes());
    hasher.update(node.as_bytes());
    *hasher.finalize().as_bytes()
}

fn shuffle_key(id: &ShardId, seed: u64) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"shardpack-shard-shuffle-v1");
    hasher.update(&seed.to_le_bytes());
    hasher.update(id.as_bytes());
    *hasher.finalize().as_bytes()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShuffleLimits {
    pub max_records: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedRecord {
    pub sample_id: [u8; 16],
    pub flags: u32,
    pub payload: Bytes,
}

pub struct ShufflePool {
    limits: ShuffleLimits,
    records: Vec<EncodedRecord>,
    bytes: usize,
    rng: XorShift64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShufflePoolCheckpoint {
    pub limits: ShuffleLimits,
    pub records: Vec<EncodedRecord>,
    pub buffered_bytes: usize,
    pub rng_state: u64,
}

impl ShufflePool {
    #[must_use]
    pub fn new(limits: ShuffleLimits, seed: u64) -> Self {
        Self {
            limits,
            records: Vec::new(),
            bytes: 0,
            rng: XorShift64(seed.max(1)),
        }
    }

    pub fn push(&mut self, record: EncodedRecord) -> Result<Option<EncodedRecord>> {
        let size = record.payload.len();
        if size > self.limits.max_bytes {
            return Err(Error::RecordTooLarge {
                bytes: size,
                limit: self.limits.max_bytes,
            });
        }
        if self.limits.max_records == 0 || self.limits.max_bytes == 0 {
            return Err(Error::RecordTooLarge {
                bytes: size,
                limit: self.limits.max_bytes,
            });
        }
        self.bytes = self.bytes.checked_add(size).ok_or(Error::InvalidLimit {
            field: "shuffle_max_bytes",
            reason: "byte accounting overflow",
        })?;
        self.records.push(record);
        if self.records.len() > self.limits.max_records || self.bytes > self.limits.max_bytes {
            let excess_bytes = self.bytes.saturating_sub(self.limits.max_bytes);
            let index = if excess_bytes == 0 {
                // Only the record-count bound was crossed, so every record is
                // eligible and selection remains O(1).
                self.rng.index(self.records.len())
            } else {
                // Reservoir-sample directly from byte-eligible records. This
                // preserves randomized eviction without allocating an index
                // vector on every push once the pool is full.
                let mut selected = None;
                let mut eligible = 0_usize;
                for (index, candidate) in self.records.iter().enumerate() {
                    if candidate.payload.len() >= excess_bytes {
                        eligible += 1;
                        if self.rng.index(eligible) == 0 {
                            selected = Some(index);
                        }
                    }
                }
                selected.expect("the newly pushed bounded record is byte-eligible")
            };
            let emitted = self.records.swap_remove(index);
            self.bytes -= emitted.payload.len();
            Ok(Some(emitted))
        } else {
            Ok(None)
        }
    }

    pub fn drain_next(&mut self) -> Option<EncodedRecord> {
        if self.records.is_empty() {
            return None;
        }
        let index = self.rng.index(self.records.len());
        let emitted = self.records.swap_remove(index);
        self.bytes -= emitted.payload.len();
        Some(emitted)
    }

    #[must_use]
    pub fn checkpoint(&self) -> ShufflePoolCheckpoint {
        ShufflePoolCheckpoint {
            limits: self.limits,
            records: self.records.clone(),
            buffered_bytes: self.bytes,
            rng_state: self.rng.0,
        }
    }

    pub fn restore(checkpoint: ShufflePoolCheckpoint) -> Result<Self> {
        Self::validate_checkpoint(&checkpoint)?;
        Ok(Self {
            limits: checkpoint.limits,
            records: checkpoint.records,
            bytes: checkpoint.buffered_bytes,
            rng: XorShift64(checkpoint.rng_state),
        })
    }

    pub fn validate_checkpoint(checkpoint: &ShufflePoolCheckpoint) -> Result<()> {
        let actual_bytes = checkpoint
            .records
            .iter()
            .try_fold(0_usize, |total, record| {
                total.checked_add(record.payload.len())
            });
        if checkpoint.limits.max_records == 0
            || checkpoint.limits.max_bytes == 0
            || checkpoint.rng_state == 0
            || actual_bytes != Some(checkpoint.buffered_bytes)
            || checkpoint.records.len() > checkpoint.limits.max_records
            || checkpoint.buffered_bytes > checkpoint.limits.max_bytes
        {
            return Err(Error::InvalidCheckpoint {
                reason: "invalid bounded shuffle pool state",
            });
        }
        Ok(())
    }

    #[must_use]
    pub const fn buffered_bytes(&self) -> usize {
        self.bytes
    }
    #[must_use]
    pub fn buffered_records(&self) -> usize {
        self.records.len()
    }
}

struct XorShift64(u64);
impl XorShift64 {
    fn index(&mut self, upper: usize) -> usize {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        (value as usize) % upper
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DistributedContext, EncodedRecord, NodeAffinity, ShuffleLimits, ShufflePool,
        affinity_assigned_shard_indices, affinity_assigned_shards, assigned_shard_indices,
        assigned_shards,
    };
    use bytes::Bytes;
    use shardpack_manifest::ShardId;

    #[test]
    fn assignment_is_disjoint_and_complete() {
        let ids: Vec<_> = (0..20).map(|n| ShardId([n; 32])).collect();
        let a = assigned_shards(
            ids.clone(),
            42,
            DistributedContext {
                rank: 0,
                world_size: 2,
                worker: 0,
                workers_per_rank: 1,
            },
        )
        .unwrap();
        let b = assigned_shards(
            ids.clone(),
            42,
            DistributedContext {
                rank: 1,
                world_size: 2,
                worker: 0,
                workers_per_rank: 1,
            },
        )
        .unwrap();
        assert!(a.iter().all(|id| !b.contains(id)));
        assert_eq!(a.len() + b.len(), ids.len());
    }

    #[test]
    fn index_assignment_matches_public_id_assignment_order() {
        let ids: Vec<_> = (0..200).map(|n| ShardId([n; 32])).collect();
        let context = DistributedContext {
            rank: 2,
            world_size: 4,
            worker: 1,
            workers_per_rank: 3,
        };
        let expected = assigned_shards(ids.clone(), 91, context).unwrap();
        let indices = assigned_shard_indices(ids.iter().copied().enumerate(), 91, context).unwrap();
        assert_eq!(
            indices
                .into_iter()
                .map(|index| ids[index])
                .collect::<Vec<_>>(),
            expected
        );

        let topology = affinity("node-b", &["node-c", "node-a", "node-b"], 1);
        let expected = affinity_assigned_shards(ids.clone(), 27, &topology).unwrap();
        let indices =
            affinity_assigned_shard_indices(ids.iter().copied().enumerate(), 27, &topology)
                .unwrap();
        assert_eq!(
            indices
                .into_iter()
                .map(|index| ids[index])
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn assignment_rejects_invalid_and_overflowing_topologies() {
        let invalid = [
            DistributedContext {
                rank: 0,
                world_size: 0,
                worker: 0,
                workers_per_rank: 1,
            },
            DistributedContext {
                rank: 1,
                world_size: 1,
                worker: 0,
                workers_per_rank: 1,
            },
            DistributedContext {
                rank: 0,
                world_size: 1,
                worker: 0,
                workers_per_rank: 0,
            },
            DistributedContext {
                rank: 0,
                world_size: 1,
                worker: 1,
                workers_per_rank: 1,
            },
            DistributedContext {
                rank: 0,
                world_size: usize::MAX,
                worker: 0,
                workers_per_rank: 2,
            },
        ];
        for context in invalid {
            assert!(matches!(
                assigned_shards([ShardId([1; 32])], 0, context),
                Err(crate::Error::InvalidTopology)
            ));
        }
    }

    #[test]
    fn more_consumers_than_shards_is_disjoint_and_complete() {
        let ids = vec![ShardId([1; 32]), ShardId([2; 32])];
        let mut observed = Vec::new();
        let mut empty_consumers = 0;
        for rank in 0..8 {
            let assigned = assigned_shards(
                ids.clone(),
                17,
                DistributedContext {
                    rank,
                    world_size: 8,
                    worker: 0,
                    workers_per_rank: 1,
                },
            )
            .unwrap();
            empty_consumers += usize::from(assigned.is_empty());
            observed.extend(assigned);
        }
        observed.sort_unstable();
        assert_eq!(observed, ids);
        assert_eq!(empty_consumers, 6);
    }

    fn affinity(node: &str, nodes: &[&str], local_rank: usize) -> NodeAffinity {
        NodeAffinity {
            node_id: node.into(),
            node_ids: nodes.iter().map(|node| (*node).into()).collect(),
            local_rank,
            local_world_size: 2,
            worker: 0,
            workers_per_rank: 1,
            affinity_seed: 9,
        }
    }

    #[test]
    fn node_affinity_is_complete_stable_and_order_independent() {
        let ids: Vec<_> = (0..200).map(|n| ShardId([n; 32])).collect();
        let nodes = ["node-a", "node-b", "node-c"];
        let mut epoch_zero = Vec::new();
        let mut epoch_one = Vec::new();
        for node in nodes {
            let mut node_epoch_zero = Vec::new();
            let mut node_epoch_one = Vec::new();
            for rank in 0..2 {
                node_epoch_zero.extend(
                    affinity_assigned_shards(ids.clone(), 0, &affinity(node, &nodes, rank))
                        .unwrap(),
                );
                node_epoch_one.extend(
                    affinity_assigned_shards(ids.clone(), 1, &affinity(node, &nodes, rank))
                        .unwrap(),
                );
            }
            node_epoch_zero.sort_unstable();
            node_epoch_one.sort_unstable();
            assert_eq!(node_epoch_zero, node_epoch_one);
            epoch_zero.extend(node_epoch_zero);
            epoch_one.extend(node_epoch_one);
        }
        epoch_zero.sort_unstable();
        epoch_one.sort_unstable();
        assert_eq!(epoch_zero, ids);
        assert_eq!(epoch_one, ids);

        let forward =
            affinity_assigned_shards(ids.clone(), 42, &affinity("node-a", &nodes, 0)).unwrap();
        let reversed = ["node-c", "node-b", "node-a"];
        let reverse = affinity_assigned_shards(ids, 42, &affinity("node-a", &reversed, 0)).unwrap();
        assert_eq!(forward, reverse);
    }

    #[test]
    fn adding_node_only_moves_shards_to_the_new_node() {
        let ids: Vec<_> = (0..=255).map(|n| ShardId([n; 32])).collect();
        let owner = |id: ShardId, nodes: &[&str]| {
            nodes
                .iter()
                .find(|node| {
                    affinity_assigned_shards(
                        [id],
                        0,
                        &NodeAffinity {
                            local_world_size: 1,
                            ..affinity(node, nodes, 0)
                        },
                    )
                    .unwrap()
                    .len()
                        == 1
                })
                .map(|node| (*node).to_owned())
                .unwrap()
        };
        let old_nodes = ["node-a", "node-b"];
        let new_nodes = ["node-a", "node-b", "node-c"];
        let mut moved = 0;
        for id in ids {
            let before = owner(id, &old_nodes);
            let after = owner(id, &new_nodes);
            if before != after {
                moved += 1;
                assert_eq!(after, "node-c");
            }
        }
        assert!(moved > 0);
    }

    #[test]
    fn shuffle_pool_respects_both_bounds() {
        let mut pool = ShufflePool::new(
            ShuffleLimits {
                max_records: 3,
                max_bytes: 8,
            },
            7,
        );
        for n in 0..20_u8 {
            let _ = pool
                .push(EncodedRecord {
                    sample_id: [n; 16],
                    flags: 0,
                    payload: Bytes::from(vec![n; 3]),
                })
                .unwrap();
            assert!(pool.buffered_records() <= 3);
            assert!(pool.buffered_bytes() <= 8);
        }

        let mut uneven = ShufflePool::new(
            ShuffleLimits {
                max_records: 10,
                max_bytes: 10,
            },
            11,
        );
        for size in [1, 1, 1, 7, 10, 2, 9, 1] {
            let _ = uneven
                .push(EncodedRecord {
                    sample_id: [size as u8; 16],
                    flags: 0,
                    payload: Bytes::from(vec![0; size]),
                })
                .unwrap();
            assert!(uneven.buffered_bytes() <= 10);
        }
    }

    #[test]
    fn shuffle_checkpoint_restores_exact_rng_and_buffered_order() {
        let limits = ShuffleLimits {
            max_records: 4,
            max_bytes: 20,
        };
        let records = (0..12_u8)
            .map(|value| EncodedRecord {
                sample_id: [value; 16],
                flags: u32::from(value),
                payload: Bytes::from(vec![value; 3]),
            })
            .collect::<Vec<_>>();
        let mut original = ShufflePool::new(limits, 73);
        for record in &records[..7] {
            let _ = original.push(record.clone()).unwrap();
        }
        let mut restored = ShufflePool::restore(original.checkpoint()).unwrap();
        let mut original_output = Vec::new();
        let mut restored_output = Vec::new();
        for record in &records[7..] {
            if let Some(value) = original.push(record.clone()).unwrap() {
                original_output.push(value.sample_id[0]);
            }
            if let Some(value) = restored.push(record.clone()).unwrap() {
                restored_output.push(value.sample_id[0]);
            }
        }
        while let Some(value) = original.drain_next() {
            original_output.push(value.sample_id[0]);
        }
        while let Some(value) = restored.drain_next() {
            restored_output.push(value.sample_id[0]);
        }
        assert_eq!(original_output, restored_output);
    }
}
