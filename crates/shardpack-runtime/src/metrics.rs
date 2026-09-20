use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Debug, Default)]
pub struct RuntimeMetrics(Arc<Inner>);

#[derive(Debug, Default)]
struct Inner {
    records_read: AtomicU64,
    encoded_bytes_read: AtomicU64,
    shards_completed: AtomicU64,
    shard_bytes_completed: AtomicU64,
    shards_verified: AtomicU64,
    shard_bytes_verified: AtomicU64,
    retries: AtomicU64,
    checksum_failures: AtomicU64,
    prefetched_shards: AtomicU64,
    queue_backpressure_events: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeMetricsSnapshot {
    pub records_read: u64,
    pub encoded_bytes_read: u64,
    pub shards_completed: u64,
    pub shard_bytes_completed: u64,
    pub shards_verified: u64,
    pub shard_bytes_verified: u64,
    pub retries: u64,
    pub checksum_failures: u64,
    pub prefetched_shards: u64,
    pub queue_backpressure_events: u64,
}

impl RuntimeMetrics {
    #[must_use]
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            records_read: self.0.records_read.load(Ordering::Relaxed),
            encoded_bytes_read: self.0.encoded_bytes_read.load(Ordering::Relaxed),
            shards_completed: self.0.shards_completed.load(Ordering::Relaxed),
            shard_bytes_completed: self.0.shard_bytes_completed.load(Ordering::Relaxed),
            shards_verified: self.0.shards_verified.load(Ordering::Relaxed),
            shard_bytes_verified: self.0.shard_bytes_verified.load(Ordering::Relaxed),
            retries: self.0.retries.load(Ordering::Relaxed),
            checksum_failures: self.0.checksum_failures.load(Ordering::Relaxed),
            prefetched_shards: self.0.prefetched_shards.load(Ordering::Relaxed),
            queue_backpressure_events: self.0.queue_backpressure_events.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_read(&self, bytes: usize) {
        self.0.records_read.fetch_add(1, Ordering::Relaxed);
        self.0
            .encoded_bytes_read
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn shard_completed(&self, bytes: u64, exact_identity_verified: bool) {
        self.0.shards_completed.fetch_add(1, Ordering::Relaxed);
        self.0
            .shard_bytes_completed
            .fetch_add(bytes, Ordering::Relaxed);
        if exact_identity_verified {
            self.0.shards_verified.fetch_add(1, Ordering::Relaxed);
            self.0
                .shard_bytes_verified
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub(crate) fn retry(&self, checksum_failure: bool) {
        self.0.retries.fetch_add(1, Ordering::Relaxed);
        if checksum_failure {
            self.0.checksum_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn prefetched_shard(&self) {
        self.0.prefetched_shards.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn queue_backpressure(&self) {
        self.0
            .queue_backpressure_events
            .fetch_add(1, Ordering::Relaxed);
    }
}
