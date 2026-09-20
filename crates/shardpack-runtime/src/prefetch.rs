use std::{collections::VecDeque, sync::Arc};

use async_stream::stream;
use bytes::Bytes;
use shardpack_cache::LocalCache;
use shardpack_manifest::{ShardId, proto};
use shardpack_storage::{AsyncObjectReader, Store, StoreRegistry};
use tokio::{
    io::{AsyncReadExt, Error as IoError},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
};
use tokio_util::io::StreamReader;

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NetworkPrefetch {
    pub concurrency: usize,
    pub max_bytes: usize,
}

pub(crate) struct PrefetchQueue {
    options: NetworkPrefetch,
    scheduled_index: usize,
    readers: VecDeque<(usize, AsyncObjectReader)>,
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
    ) -> Result<Option<AsyncObjectReader>> {
        self.fill(stores, cache, shards, shard_order)?;
        let Some((index, reader)) = self.readers.pop_front() else {
            return Ok(None);
        };
        if index != expected_index {
            return Err(Error::InvalidCheckpoint {
                reason: "network prefetch queue is out of order",
            });
        }
        self.fill(stores, cache, shards, shard_order)?;
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
) -> AsyncObjectReader {
    type Chunk = std::result::Result<(Bytes, OwnedSemaphorePermit), IoError>;
    let semaphore = Arc::new(Semaphore::new(byte_limit));
    let chunk_bytes = byte_limit.min(1024 * 1024);
    let buffered_chunks = byte_limit.div_ceil(chunk_bytes);
    let (sender, mut receiver) = mpsc::channel::<Chunk>(buffered_chunks);
    tokio::spawn(async move {
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
            let mut buffer = vec![0_u8; chunk_bytes];
            match source.read(&mut buffer).await {
                Ok(0) => return,
                Ok(read) => {
                    buffer.truncate(read);
                    if sender
                        .send(Ok((Bytes::from(buffer), permit)))
                        .await
                        .is_err()
                    {
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
    Box::pin(StreamReader::new(stream))
}
