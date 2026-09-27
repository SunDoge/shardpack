//! Warm filesystem plaintext cache throughput, including identity verification.
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use shardpack_cache::LocalCache;
use shardpack_format::{AsyncShardReader, EncryptionKey, ShardWriter, WriterOptions};
use shardpack_manifest::ShardId;
use shardpack_storage::Store;
use tokio::io::AsyncReadExt;

const RECORDS: usize = 1024;
const RECORD_BYTES: usize = 64 * 1024;

fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

async fn run() {
    let root = tempfile::tempdir().unwrap();
    let remote = root.path().join("remote");
    std::fs::create_dir(&remote).unwrap();
    let store = Store::filesystem(&remote).unwrap();
    let key = EncryptionKey::new([7; 32]);
    let mut writer =
        ShardWriter::new_encrypted(Vec::new(), WriterOptions::default(), key.clone()).unwrap();
    let sample = vec![42; RECORD_BYTES];
    for index in 0..RECORDS {
        let mut id = [0; 16];
        id[..8].copy_from_slice(&(index as u64).to_le_bytes());
        writer.add_record(id, 0, &sample).unwrap();
    }
    let (bytes, summary) = writer.finish().unwrap();
    std::fs::write(remote.join("input.spk"), bytes).unwrap();
    let id = ShardId(summary.shard_id);
    let cache = LocalCache::new(root.path().join("cache")).with_plaintext(key);
    // Populate, authenticate, and finalize the cache before timing.
    let mut source = cache
        .open(&store, "input.spk", id, summary.byte_size)
        .await
        .unwrap();
    tokio::io::copy(&mut source, &mut tokio::io::sink())
        .await
        .unwrap();
    drop(source);
    for mode in ["chunks", "records"] {
        let mut destination = vec![0; 1024 * 1024];
        let mut started = Instant::now();
        let mut rounds = 0;
        loop {
            let mut source = cache
                .open(&store, "input.spk", id, summary.byte_size)
                .await
                .unwrap();
            if mode == "chunks" {
                while source.read(&mut destination).await.unwrap() != 0 {
                    black_box(&destination);
                }
            } else {
                let mut reader = AsyncShardReader::new(source)
                    .await
                    .unwrap()
                    .without_content_fingerprint();
                let mut count = 0;
                while let Some(record) = reader.next_record().await.unwrap() {
                    assert_eq!(record.payload.len(), RECORD_BYTES);
                    assert_eq!(record.payload[0], 42);
                    black_box(record);
                    count += 1;
                }
                assert_eq!(count, RECORDS);
            }
            if rounds == 0 {
                started = Instant::now(); // first iteration is warmup
                rounds = 1;
            } else {
                rounds += 1;
                if started.elapsed() >= Duration::from_secs(1) {
                    break;
                }
            }
        }
        println!(
            "{mode},{:.6}",
            (rounds - 1) as f64 * (RECORDS * RECORD_BYTES) as f64
                / started.elapsed().as_secs_f64()
                / 1024_f64.powi(3)
        );
    }
}
