//! Bounded recent-history sampling for multi-record augmentation, not shuffle.
use std::collections::VecDeque;

use snafu::Snafu;

use crate::{EncodedRecord, XorShift64, compact_payload};

#[derive(Clone, Copy, Debug)]
pub struct SamplePoolLimits {
    pub max_records: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Snafu)]
pub enum SamplePoolError {
    #[snafu(display("sample pool limits must be positive"))]
    InvalidLimits,
    #[snafu(display("sample payload {bytes} exceeds pool byte limit {limit}"))]
    RecordTooLarge { bytes: usize, limit: usize },
    #[snafu(display("sample count {count} exceeds pool record limit {limit}"))]
    CountTooLarge { count: usize, limit: usize },
    #[snafu(display("requested {requested} samples but only {available} eligible records exist"))]
    InsufficientRecords { requested: usize, available: usize },
}

pub struct SamplePool {
    limits: SamplePoolLimits,
    records: VecDeque<EncodedRecord>,
    bytes: usize,
    rng: XorShift64,
}

impl SamplePool {
    pub fn new(limits: SamplePoolLimits, seed: u64) -> Result<Self, SamplePoolError> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(SamplePoolError::InvalidLimits);
        }
        let mut pool = Self {
            limits,
            records: VecDeque::new(),
            bytes: 0,
            rng: XorShift64(1),
        };
        pool.reset(seed);
        Ok(pool)
    }

    /// Drop pool references and reset its independent, domain-separated RNG.
    pub fn reset(&mut self, seed: u64) {
        self.records.clear();
        self.bytes = 0;
        let mut hash = blake3::Hasher::new();
        hash.update(b"shardpack-sample-pool-v1");
        hash.update(&seed.to_le_bytes());
        self.rng = XorShift64(
            u64::from_le_bytes(hash.finalize().as_bytes()[..8].try_into().unwrap()).max(1),
        );
    }

    /// Keep one compact payload per SampleId, refreshing duplicates to newest.
    /// FIFO eviction is independent of sampling. Oversized insertions are atomic.
    pub fn insert(&mut self, mut record: EncodedRecord) -> Result<(), SamplePoolError> {
        let size = record.payload.len();
        if size > self.limits.max_bytes {
            return Err(SamplePoolError::RecordTooLarge {
                bytes: size,
                limit: self.limits.max_bytes,
            });
        }
        compact_payload(&mut record);
        if let Some(index) = self
            .records
            .iter()
            .position(|r| r.sample_id == record.sample_id)
        {
            self.bytes -= self.records.remove(index).unwrap().payload.len();
        }
        while self.records.len() >= self.limits.max_records
            || self.bytes > self.limits.max_bytes - size
        {
            self.bytes -= self.records.pop_front().unwrap().payload.len();
        }
        self.bytes += size;
        self.records.push_back(record);
        Ok(())
    }

    pub fn available(&self, exclude_ids: &[[u8; 16]]) -> usize {
        self.records
            .iter()
            .filter(|r| !exclude_ids.contains(&r.sample_id))
            .count()
    }

    /// Sampling does not remove/refresh entries. Returned Bytes own their data
    /// even after eviction/reset. Output count is also bounded by max_records.
    pub fn sample(
        &mut self,
        count: usize,
        exclude_ids: &[[u8; 16]],
        replace: bool,
    ) -> Result<Vec<EncodedRecord>, SamplePoolError> {
        if count > self.limits.max_records {
            return Err(SamplePoolError::CountTooLarge {
                count,
                limit: self.limits.max_records,
            });
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut eligible: Vec<_> = self
            .records
            .iter()
            .enumerate()
            .filter(|(_, r)| !exclude_ids.contains(&r.sample_id))
            .map(|(i, _)| i)
            .collect();
        if eligible.is_empty() || (!replace && count > eligible.len()) {
            return Err(SamplePoolError::InsufficientRecords {
                requested: count,
                available: eligible.len(),
            });
        }
        let mut output = Vec::with_capacity(count);
        for _ in 0..count {
            let index = self.rng.index(eligible.len());
            let selected = if replace {
                eligible[index]
            } else {
                eligible.swap_remove(index)
            };
            output.push(self.records[selected].clone());
        }
        Ok(output)
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub fn buffered_bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    fn record(id: u8, size: usize) -> EncodedRecord {
        EncodedRecord {
            sample_id: [id; 16],
            flags: u32::from(id),
            payload: Bytes::from(vec![id; size]),
        }
    }
    fn pool() -> SamplePool {
        SamplePool::new(
            SamplePoolLimits {
                max_records: 4,
                max_bytes: 8,
            },
            7,
        )
        .unwrap()
    }
    #[test]
    fn bounds_duplicates_and_oversize_are_explicit() {
        assert!(
            SamplePool::new(
                SamplePoolLimits {
                    max_records: 0,
                    max_bytes: 1
                },
                0
            )
            .is_err()
        );
        let mut p = pool();
        for i in 0..20 {
            p.insert(record(i, 3)).unwrap();
            assert!(p.buffered_bytes() <= 8);
        }
        assert_eq!(p.len(), 2);
        p.insert(record(19, 5)).unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!(p.buffered_bytes(), 8);
        assert!(p.insert(record(99, 9)).is_err());
        assert_eq!(p.buffered_bytes(), 8);
        assert_eq!(p.available(&[[19; 16]]), 1);
        p.insert(record(20, 8)).unwrap();
        assert_eq!(p.len(), 1);
    }
    #[test]
    fn deterministic_non_destructive_and_handles_survive_eviction() {
        let mut a = pool();
        let mut b = pool();
        for i in 0..4 {
            a.insert(record(i, 2)).unwrap();
            b.insert(record(i, 2)).unwrap();
        }
        assert!(a.sample(4, &[[0; 16]], false).is_err()); // failure must not advance RNG
        let saved = a.sample(3, &[[0; 16]], false).unwrap();
        assert_eq!(saved, b.sample(3, &[[0; 16]], false).unwrap());
        let mut ids: Vec<_> = saved.iter().map(|r| r.sample_id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 3);
        assert_eq!(a.len(), 4);
        for i in 4..20 {
            a.insert(record(i, 2)).unwrap();
        }
        a.reset(7);
        assert!(a.is_empty());
        for r in saved {
            assert_eq!(r.payload.as_ref(), &[r.sample_id[0]; 2]);
        }
        assert!(a.sample(1, &[], true).is_err());
        assert!(a.sample(0, &[], false).unwrap().is_empty());
        a.insert(record(42, 1)).unwrap();
        assert_eq!(a.sample(4, &[], true).unwrap(), vec![record(42, 1); 4]);
        assert!(a.sample(5, &[], true).is_err());
    }
    #[test]
    fn insert_does_not_pin_source_block() {
        let mut p = pool();
        let source = Bytes::from(vec![3; 1024 * 1024]);
        let mut r = record(3, 1);
        r.payload = source.slice(100..102);
        p.insert(r).unwrap();
        assert!(source.is_unique());
        assert_eq!(p.buffered_bytes(), 2);
    }
}
