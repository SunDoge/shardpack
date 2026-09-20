use bytes::Bytes;
use futures_util::{StreamExt, stream};
use shardpack_storage::{Error, S3Options, Store};
use std::sync::Arc;
use tokio::sync::Barrier;

/// Runs against RustFS, MinIO, or a real S3 bucket. It is ignored by default because it
/// creates and deletes a uniquely named object in the configured bucket.
///
/// Required: SHARDPACK_TEST_S3_BUCKET. Optional: SHARDPACK_TEST_S3_ENDPOINT,
/// SHARDPACK_TEST_S3_ROOT, AWS_REGION and the normal AWS credential variables.
#[tokio::test]
#[ignore = "requires an explicitly configured disposable S3-compatible bucket"]
async fn s3_stream_range_list_conditional_create_and_delete() {
    let bucket = std::env::var("SHARDPACK_TEST_S3_BUCKET").expect("test bucket is required");
    let options = S3Options {
        bucket,
        root: std::env::var("SHARDPACK_TEST_S3_ROOT").ok(),
        endpoint: std::env::var("SHARDPACK_TEST_S3_ENDPOINT").ok(),
        region: std::env::var("AWS_REGION").ok(),
        profile: None,
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
        session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        ..S3Options::default()
    };
    let store = Store::s3(&options).unwrap();
    // This prefix is reserved for this ignored integration test. Cleaning it
    // first also recovers objects left by a previously interrupted test run.
    for stale in store.list_keys("integration/").await.unwrap() {
        store.delete(&stale).await.unwrap();
    }
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("integration/{}/{}-{nonce}", std::process::id(), line!());
    let key = format!("{prefix}/stream.bin");
    let part_bytes = 5 * 1024 * 1024;
    let expected_bytes = 4 * part_bytes;

    store
        .write_immutable(
            &key,
            Box::pin(stream::iter(
                (0..4_u8).map(move |value| Ok(Bytes::from(vec![value; part_bytes]))),
            )),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .read_range(&key, part_bytes as u64 - 2..part_bytes as u64 + 2)
            .await
            .unwrap(),
        Bytes::from_static(&[0, 0, 1, 1])
    );
    let mut resumed = store
        .open_reader_from(&key, (expected_bytes - 16) as u64)
        .await
        .unwrap();
    let mut resumed_bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut resumed, &mut resumed_bytes)
        .await
        .unwrap();
    assert_eq!(resumed_bytes, vec![3; 16]);
    let chunks = store
        .stream(&key)
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let downloaded = chunks.concat();
    assert_eq!(downloaded.len(), expected_bytes);
    for (index, chunk) in downloaded.chunks_exact(part_bytes).enumerate() {
        assert!(chunk.iter().all(|byte| *byte == index as u8));
    }
    assert!(store.list_keys(&prefix).await.unwrap().contains(&key));

    let duplicate = store
        .write_immutable(
            &key,
            Box::pin(stream::once(async { Ok(Bytes::from_static(b"replace")) })),
        )
        .await;
    let duplicate_rejected = matches!(duplicate, Err(Error::AlreadyExists { .. }));
    let original_preserved = store.read_all(&key).await.unwrap().len() == expected_bytes;

    let concurrent_key = format!("{prefix}/concurrent-create.bin");
    let barrier = Arc::new(Barrier::new(8));
    let mut tasks = Vec::new();
    for value in 0..8_u8 {
        let store = store.clone();
        let key = concurrent_key.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .write_immutable(
                    &key,
                    Box::pin(stream::once(
                        async move { Ok(Bytes::from(vec![value; 32])) },
                    )),
                )
                .await
        }));
    }
    let mut created = 0;
    let mut rejected = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(()) => created += 1,
            Err(Error::AlreadyExists { .. }) => rejected += 1,
            Err(error) => panic!("unexpected concurrent-create error: {error}"),
        }
    }
    let concurrent_object_valid = store.read_all(&concurrent_key).await.unwrap().len() == 32;

    store.delete(&key).await.unwrap();
    store.delete(&concurrent_key).await.unwrap();
    assert!(matches!(
        store.read_all(&key).await,
        Err(Error::NotFound { .. })
    ));
    assert!(
        duplicate_rejected && original_preserved,
        "backend did not enforce immutable conditional create: result={duplicate:?}, original_preserved={original_preserved}"
    );
    assert_eq!(created, 1, "backend allowed multiple concurrent creators");
    assert_eq!(rejected, 7, "backend did not reject concurrent creators");
    assert!(concurrent_object_valid);
}
