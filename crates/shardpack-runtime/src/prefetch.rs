use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_stream::stream;
use bytes::{Bytes, BytesMut};
use shardpack_cache::LocalCache;
use shardpack_manifest::{ShardId, proto};
use shardpack_storage::{AsyncObjectReader, Store, StoreRegistry};
use tokio::{
    io::{AsyncRead, AsyncReadExt, Error as IoError, ReadBuf},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
};
use tokio_util::io::StreamReader;

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NetworkPrefetch {
    /// Maximum active shard streams, including the stream currently consumed.
    pub concurrency: usize,
    /// Total queued chunk budget across all active streams. Does not include
    /// object-store internals, decoded blocks or the cache's current block.
    pub max_bytes: usize,
}

pub(crate) struct PrefetchQueue {
    options: NetworkPrefetch,
    scheduled_index: usize,
    readers: VecDeque<(usize, PrefetchedObject)>,
}

pub(crate) struct PrefetchedObject {
    pub(crate) reader: AsyncObjectReader,
    pub(crate) plaintext: bool,
}

impl PrefetchQueue {
    pub(crate) fn new(options: NetworkPrefetch, start_index: usize) -> Result<Self> {
        if options.concurrency == 0 {
            return Err(Error::InvalidLimit {
                field: "network_concurrency",
                reason: "must be greater than zero",
            });
        }
        if options.max_bytes < options.concurrency || options.max_bytes > u32::MAX as usize {
            return Err(Error::InvalidLimit {
                field: "network_prefetch_bytes",
                reason: "must be at least network_concurrency and fit in u32",
            });
        }
        Ok(Self {
            options,
            scheduled_index: start_index,
            readers: VecDeque::new(),
        })
    }

    pub(crate) fn options(&self) -> NetworkPrefetch {
        self.options
    }

    pub(crate) fn take_next(
        &mut self,
        expected_index: usize,
        stores: &StoreRegistry,
        cache: Option<&LocalCache>,
        shards: &[proto::ShardMeta],
        shard_order: &[usize],
    ) -> Result<Option<PrefetchedObject>> {
        self.fill(stores, cache, shards, shard_order)?;
        let Some((index, reader)) = self.readers.pop_front() else {
            return Ok(None);
        };
        if index != expected_index {
            return Err(Error::InvalidCheckpoint {
                reason: "network prefetch queue is out of order",
            });
        }
        Ok(Some(reader))
    }

    fn fill(
        &mut self,
        stores: &StoreRegistry,
        cache: Option<&LocalCache>,
        shards: &[proto::ShardMeta],
        shard_order: &[usize],
    ) -> Result<()> {
        let per_reader_bytes = self.options.max_bytes / self.options.concurrency;
        while self.readers.len() < self.options.concurrency
            && self.scheduled_index < shard_order.len()
        {
            let index = self.scheduled_index;
            let meta = &shards[shard_order[index]];
            let shard_id =
                ShardId::from_slice(&meta.shard_id).map_err(|source| Error::Manifest { source })?;
            let location = shardpack_manifest::object_location(meta);
            let store = stores
                .get(&location.store_id)
                .map_err(|source| Error::Storage { source })?;
            self.readers.push_back((
                index,
                spawn_prefetch(
                    store.clone(),
                    cache.cloned(),
                    location.object_key.clone(),
                    shard_id,
                    meta.byte_size,
                    per_reader_bytes,
                ),
            ));
            self.scheduled_index += 1;
        }
        Ok(())
    }
}

fn spawn_prefetch(
    store: Store,
    cache: Option<LocalCache>,
    object_key: String,
    shard_id: ShardId,
    expected_bytes: u64,
    byte_limit: usize,
) -> PrefetchedObject {
    type Chunk = std::result::Result<(Bytes, OwnedSemaphorePermit), IoError>;
    let semaphore = Arc::new(Semaphore::new(byte_limit));
    let chunk_bytes = byte_limit.min(1024 * 1024);
    let buffered_chunks = byte_limit.div_ceil(chunk_bytes);
    let (sender, mut receiver) = mpsc::channel::<Chunk>(buffered_chunks);
    let plaintext = cache
        .as_ref()
        .is_some_and(|cache| cache.mode() == shardpack_cache::CacheMode::Plaintext);
    let task = tokio::spawn(async move {
        let source = match cache {
            Some(cache) => cache
                .open(&store, &object_key, shard_id, expected_bytes)
                .await
                .map_err(IoError::other),
            None => store.open_reader(&object_key).await.map_err(IoError::other),
        };
        let mut source = match source {
            Ok(source) => source,
            Err(error) => {
                let _ = sender.send(Err(error)).await;
                return;
            }
        };
        loop {
            let permit = match semaphore
                .clone()
                .acquire_many_owned(chunk_bytes as u32)
                .await
            {
                Ok(permit) => permit,
                Err(_) => return,
            };
            let mut buffer = BytesMut::with_capacity(chunk_bytes);
            match read_chunk(&mut source, &mut buffer, chunk_bytes).await {
                Ok(0) => return,
                Ok(read) => {
                    debug_assert_eq!(buffer.len(), read);
                    if sender.send(Ok((buffer.freeze(), permit))).await.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            }
        }
    });
    let stream = stream! {
        while let Some(item) = receiver.recv().await {
            match item {
                Ok((bytes, permit)) => {
                    yield Ok::<Bytes, IoError>(bytes);
                    drop(permit);
                }
                Err(error) => {
                    yield Err(error);
                    break;
                }
            }
        }
    };
    PrefetchedObject {
        reader: Box::pin(CancelOnDropReader {
            reader: Box::pin(StreamReader::new(stream)),
            task,
        }),
        plaintext,
    }
}

// Coalesce short reads (including plaintext cache headers) into budget-sized
// chunks. read_buf initializes only bytes actually read, avoiding a zero-fill.
async fn read_chunk(
    source: &mut AsyncObjectReader,
    buffer: &mut BytesMut,
    limit: usize,
) -> std::io::Result<usize> {
    while buffer.len() < limit {
        let remaining = limit - buffer.len();
        if source.take(remaining as u64).read_buf(buffer).await? == 0 {
            break;
        }
    }
    Ok(buffer.len())
}

struct CancelOnDropReader {
    reader: AsyncObjectReader,
    task: tokio::task::JoinHandle<()>,
}

impl AsyncRead for CancelOnDropReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.reader.as_mut().poll_read(cx, buffer)
    }
}

impl Drop for CancelOnDropReader {
    fn drop(&mut self) {
        // Also cancel a stalled object open/read, not only a blocked channel send.
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn active_reader_counts_toward_concurrency() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::filesystem(root.path()).unwrap();
        let mut stores = StoreRegistry::new();
        stores.insert("default", store).unwrap();
        let shards: Vec<_> = (0..4)
            .map(|i| proto::ShardMeta {
                shard_id: vec![i; 32],
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: format!("{i}.spk"),
                }),
                ..Default::default()
            })
            .collect();
        let mut queue = PrefetchQueue::new(
            NetworkPrefetch {
                concurrency: 2,
                max_bytes: 64,
            },
            0,
        )
        .unwrap();
        for i in 0..4 {
            let active = queue
                .take_next(i, &stores, None, &shards, &[0, 1, 2, 3])
                .unwrap()
                .unwrap();
            assert!(queue.readers.len() < 2);
            drop(active);
        }
    }

    #[tokio::test]
    async fn coalesces_short_reads_and_preserves_partial_tail() {
        let pieces = futures_util::stream::iter([
            Ok::<_, IoError>(Bytes::from_static(b"a")),
            Ok(Bytes::from_static(b"bc")),
            Ok(Bytes::from_static(b"def")),
        ]);
        let mut reader: AsyncObjectReader = Box::pin(StreamReader::new(pieces));
        let mut buffer = BytesMut::with_capacity(4);
        assert_eq!(read_chunk(&mut reader, &mut buffer, 4).await.unwrap(), 4);
        assert_eq!(&buffer[..], b"abcd");
        buffer.clear();
        assert_eq!(read_chunk(&mut reader, &mut buffer, 4).await.unwrap(), 2);
        assert_eq!(&buffer[..], b"ef");
        buffer.clear();
        assert_eq!(read_chunk(&mut reader, &mut buffer, 4).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn dropping_reader_cancels_stalled_task() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _guard = tx;
            std::future::pending::<()>().await;
        });
        let reader = CancelOnDropReader {
            reader: Box::pin(tokio::io::empty()),
            task,
        };
        drop(reader);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), rx)
                .await
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    async fn partial_chunk_error_is_not_silently_treated_as_eof() {
        let pieces = futures_util::stream::iter([
            Ok(Bytes::from_static(b"abc")),
            Err(IoError::other("read failed")),
        ]);
        let mut reader: AsyncObjectReader = Box::pin(StreamReader::new(pieces));
        let error = read_chunk(&mut reader, &mut BytesMut::new(), 4)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "read failed");
    }
}
