//! Counts live source-block allocations independently of allocator/RSS caching.
use bytes::Bytes;
use shardpack_runtime::{EncodedRecord, ShuffleLimits, ShufflePool};
use std::{
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

struct Block {
    bytes: Vec<u8>,
    live: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Block {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        self.live.fetch_sub(self.bytes.len(), Ordering::Relaxed);
    }
}

fn main() {
    const BLOCK: usize = 4 * 1024 * 1024;
    const RECORD: usize = 64 * 1024;
    const BLOCKS: usize = 128;
    let live = Arc::new(AtomicUsize::new(0));
    let mut pool = ShufflePool::new(
        ShuffleLimits {
            max_records: 1024,
            max_bytes: 256 * 1024 * 1024,
        },
        7,
    );
    let started = Instant::now();
    let mut peak = 0;
    let mut count = 0;
    for n in 0..BLOCKS {
        live.fetch_add(BLOCK, Ordering::Relaxed);
        let block = Bytes::from_owner(Block {
            bytes: vec![n as u8; BLOCK],
            live: live.clone(),
        });
        for offset in (0..BLOCK).step_by(RECORD) {
            if let Some(record) = pool
                .push(EncodedRecord {
                    sample_id: [n as u8; 16],
                    flags: 0,
                    payload: block.slice(offset..offset + RECORD),
                })
                .unwrap()
            {
                assert_eq!(record.payload[0], record.sample_id[0]);
                black_box(record);
                count += 1;
            }
        }
        drop(block);
        peak = peak.max(live.load(Ordering::Relaxed));
    }
    println!("peak_pinned_source_bytes,{peak}");
    println!("retained_payload_bytes,{}", pool.buffered_bytes());
    while let Some(record) = pool.drain_next() {
        black_box(record);
        count += 1;
    }
    assert_eq!(count, BLOCKS * BLOCK / RECORD);
    assert_eq!(live.load(Ordering::Relaxed), 0);
    println!(
        "logical_gib_s,{:.6}",
        (BLOCKS * BLOCK) as f64 / started.elapsed().as_secs_f64() / 1024_f64.powi(3)
    );
}
