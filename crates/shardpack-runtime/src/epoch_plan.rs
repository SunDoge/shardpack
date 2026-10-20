//! Plan an epoch before starting collectives. No silent padding or truncation.
use shardpack_manifest::{ShardId, proto};

use crate::{DistributedContext, Error, Result, assigned_shard_indices};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EpochPlan {
    pub records_per_rank: Vec<u64>,
    pub batches_per_rank: Vec<u64>,
}

impl EpochPlan {
    pub fn build(
        shards: &[proto::ShardMeta],
        shard_seed: u64,
        world_size: usize,
        batch_size: u64,
        drop_last: bool,
    ) -> Result<Self> {
        if world_size == 0 || batch_size == 0 {
            return Err(Error::InvalidTopology);
        }
        let ids = shards
            .iter()
            .enumerate()
            .map(|(i, shard)| {
                ShardId::from_slice(&shard.shard_id)
                    .map(|id| (i, id))
                    .map_err(|source| Error::Manifest { source })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut records_per_rank = vec![0_u64; world_size];
        // Sort once, then apply the same round-robin partition as the reader.
        let indices = assigned_shard_indices(
            ids,
            shard_seed,
            DistributedContext {
                rank: 0,
                world_size: 1,
                worker: 0,
                workers_per_rank: 1,
            },
        )?;
        for (position, i) in indices.into_iter().enumerate() {
            let count = &mut records_per_rank[position % world_size];
            *count = count
                .checked_add(shards[i].record_count)
                .ok_or(Error::InvalidLimit {
                    field: "record_count",
                    reason: "rank record count overflows u64",
                })?;
        }
        let batches_per_rank = records_per_rank
            .iter()
            .map(|&n| {
                if drop_last {
                    n / batch_size
                } else {
                    n.div_ceil(batch_size)
                }
            })
            .collect();
        Ok(Self {
            records_per_rank,
            batches_per_rank,
        })
    }

    pub fn require_equal_steps(&self) -> Result<u64> {
        let first = self.batches_per_rank.first().copied().unwrap_or(0);
        if first == 0 || self.batches_per_rank.iter().any(|&n| n != first) {
            return Err(Error::InvalidLimit {
                field: "epoch_plan",
                reason: "ranks have unequal or zero steps; explicit balancing is required",
            });
        }
        Ok(first)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn detects_uneven_steps_even_with_drop_last() {
        let shards = [3, 9]
            .into_iter()
            .enumerate()
            .map(|(i, n)| proto::ShardMeta {
                shard_id: vec![i as u8; 32],
                record_count: n,
                ..Default::default()
            })
            .collect::<Vec<_>>();
        for drop_last in [false, true] {
            let plan = EpochPlan::build(&shards, 42, 2, 2, drop_last).unwrap();
            assert_eq!(plan.records_per_rank.iter().sum::<u64>(), 12);
            assert!(plan.require_equal_steps().is_err());
        }
        assert!(
            EpochPlan::build(&shards, 42, 4, 2, false)
                .unwrap()
                .require_equal_steps()
                .is_err()
        );
        assert_eq!(
            EpochPlan::build(&shards, 42, 1, 2, false)
                .unwrap()
                .require_equal_steps()
                .unwrap(),
            6
        );
    }
}
