use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::{StreamExt, stream};
use shardpack_storage::{S3Options, Store};

const CHUNK_BYTES: usize = 8 * 1024 * 1024;
const CHUNKS: usize = 32;

#[tokio::test]
#[ignore = "requires an explicitly configured disposable S3-compatible bucket"]
async fn report_s3_sequential_throughput() {
    let bucket = std::env::var("SHARDPACK_TEST_S3_BUCKET").expect("test bucket is required");
    let defaults = S3Options::default();
    let store = Store::s3(&S3Options {
        bucket,
        root: std::env::var("SHARDPACK_TEST_S3_ROOT").ok(),
        endpoint: std::env::var("SHARDPACK_TEST_S3_ENDPOINT").ok(),
        region: std::env::var("AWS_REGION").ok(),
        profile: None,
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
        session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        write_chunk_bytes: env_usize(
            "SHARDPACK_TEST_S3_WRITE_CHUNK_BYTES",
            defaults.write_chunk_bytes,
        ),
        write_concurrency: env_usize(
            "SHARDPACK_TEST_S3_WRITE_CONCURRENCY",
            defaults.write_concurrency,
        ),
    })
    .unwrap();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let key = format!("benchmark/{}/{}-{nonce}.bin", std::process::id(), line!());
    let chunk = Bytes::from(vec![0x5a; CHUNK_BYTES]);
    let total_bytes = CHUNK_BYTES * CHUNKS;

    let started = Instant::now();
    store
        .write_immutable(
            &key,
            Box::pin(stream::iter((0..CHUNKS).map(move |_| Ok(chunk.clone())))),
        )
        .await
        .unwrap();
    report("upload", total_bytes, started.elapsed());

    let started = Instant::now();
    let mut downloaded = 0_usize;
    let mut input = store.stream(&key).await.unwrap();
    while let Some(part) = input.next().await {
        downloaded += part.unwrap().len();
    }
    assert_eq!(downloaded, total_bytes);
    report("download", total_bytes, started.elapsed());

    store.delete(&key).await.unwrap();
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().expect("throughput tuning must be an integer"))
        .unwrap_or(default)
}

fn report(operation: &str, bytes: usize, elapsed: std::time::Duration) {
    let mib_per_second = bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    println!("RustFS {operation}: {mib_per_second:.1} MiB/s ({elapsed:?})");
}
