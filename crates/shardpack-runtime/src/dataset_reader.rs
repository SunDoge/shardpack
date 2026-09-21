use std::sync::Arc;

use shardpack_cache::LocalCache;
use shardpack_format::{AsyncReaderPosition, AsyncShardReader, Blake3Reader, ReaderLimits};
use shardpack_manifest::{ShardId, proto};
use shardpack_storage::{AsyncObjectReader, Store, StoreRegistry};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    DistributedContext, EncodedRecord, Error, NetworkPrefetch, NodeAffinity, Result,
    RuntimeMetrics, affinity_assigned_shard_indices, assigned_shard_indices,
    prefetch::{PrefetchQueue, PrefetchedObject},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Number of retries after the initial attempt.
    pub max_retries: u32,
    pub initial_backoff: std::time::Duration,
    pub max_backoff: std::time::Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: std::time::Duration::from_millis(100),
            max_backoff: std::time::Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaderPosition {
    pub next_shard_index: usize,
    pub current_shard_id: Option<[u8; 32]>,
    pub records_yielded_in_current_shard: u64,
}

/// Durable coordinates for restarting a pinned loader. Offsets always point
/// at a block header, never into compressed payload data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoaderCheckpoint {
    pub snapshot_id: [u8; 32],
    pub layout_id: [u8; 32],
    pub epoch: u64,
    pub shard_seed: u64,
    pub next_shard_index: usize,
    pub current_shard_id: Option<[u8; 32]>,
    pub records_yielded_in_current_shard: u64,
    pub block_index: u32,
    pub block_start_offset: u64,
    pub record_index: u32,
    pub distributed: Option<DistributedContext>,
    pub node_affinity: Option<NodeAffinity>,
}

#[derive(Clone, Copy)]
struct RetryPosition {
    next_shard_index: usize,
    current_shard_id: Option<[u8; 32]>,
    records_yielded_in_current_shard: u64,
    reader: AsyncReaderPosition,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IncompleteLayoutPolicy {
    #[default]
    Reject,
    Allow,
}

pub struct DatasetReader {
    stores: StoreRegistry,
    layout: Arc<proto::LayoutManifest>,
    shard_order: Vec<usize>,
    next_shard_index: usize,
    current_shard_index: Option<usize>,
    current: Option<AsyncShardReader<Blake3Reader<AsyncObjectReader>>>,
    cache: Option<LocalCache>,
    epoch: u64,
    shard_seed: u64,
    records_before_current_reader: u64,
    verify_current_fingerprint: bool,
    retry_policy: RetryPolicy,
    prefetch: Option<PrefetchQueue>,
    metrics: RuntimeMetrics,
    distributed: Option<DistributedContext>,
    node_affinity: Option<NodeAffinity>,
    decryption_key: Option<shardpack_format::EncryptionKey>,
}

impl DatasetReader {
    pub fn new(store: Store, layout: impl Into<Arc<proto::LayoutManifest>>) -> Result<Self> {
        Self::new_with_incomplete_policy(store, layout, IncompleteLayoutPolicy::Reject)
    }

    pub fn new_with_incomplete_policy(
        store: Store,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        incomplete_policy: IncompleteLayoutPolicy,
    ) -> Result<Self> {
        let layout = layout.into();
        shardpack_manifest::validate_layout(&layout)
            .map_err(|source| Error::Manifest { source })?;
        let stores = registry_for_single_store(store, &layout)?;
        Self::new_with_registry_and_incomplete_policy(stores, layout, incomplete_policy)
    }

    pub fn new_with_registry(
        stores: StoreRegistry,
        layout: impl Into<Arc<proto::LayoutManifest>>,
    ) -> Result<Self> {
        Self::new_with_registry_and_incomplete_policy(
            stores,
            layout,
            IncompleteLayoutPolicy::Reject,
        )
    }

    pub fn new_with_registry_and_incomplete_policy(
        stores: StoreRegistry,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        incomplete_policy: IncompleteLayoutPolicy,
    ) -> Result<Self> {
        let layout = layout.into();
        shardpack_manifest::validate_layout(&layout)
            .map_err(|source| Error::Manifest { source })?;
        for shard in &layout.shards {
            let location = shardpack_manifest::object_location(shard);
            stores
                .get(&location.store_id)
                .map_err(|source| Error::Storage { source })?;
        }
        if proto::LayoutState::try_from(layout.state) == Ok(proto::LayoutState::Incomplete)
            && incomplete_policy == IncompleteLayoutPolicy::Reject
        {
            return Err(Error::IncompleteLayout);
        }
        let shard_order = (0..layout.shards.len()).collect();
        Ok(Self {
            stores,
            layout,
            shard_order,
            next_shard_index: 0,
            current_shard_index: None,
            current: None,
            cache: None,
            epoch: 0,
            shard_seed: 0,
            records_before_current_reader: 0,
            verify_current_fingerprint: true,
            retry_policy: RetryPolicy::default(),
            prefetch: None,
            metrics: RuntimeMetrics::default(),
            distributed: None,
            node_affinity: None,
            decryption_key: None,
        })
    }

    pub fn new_assigned(
        store: Store,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        epoch: u64,
        shard_seed: u64,
        context: DistributedContext,
    ) -> Result<Self> {
        Self::new_assigned_with_incomplete_policy(
            store,
            layout,
            epoch,
            shard_seed,
            context,
            IncompleteLayoutPolicy::Reject,
        )
    }

    pub fn new_assigned_with_incomplete_policy(
        store: Store,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        epoch: u64,
        shard_seed: u64,
        context: DistributedContext,
        incomplete_policy: IncompleteLayoutPolicy,
    ) -> Result<Self> {
        let mut reader = Self::new_with_incomplete_policy(store, layout, incomplete_policy)?;
        reader.apply_distributed_assignment(shard_seed, context)?;
        reader.epoch = epoch;
        Ok(reader)
    }

    pub fn new_assigned_with_registry(
        stores: StoreRegistry,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        epoch: u64,
        shard_seed: u64,
        context: DistributedContext,
    ) -> Result<Self> {
        let mut reader = Self::new_with_registry(stores, layout)?;
        reader.apply_distributed_assignment(shard_seed, context)?;
        reader.epoch = epoch;
        Ok(reader)
    }

    fn apply_distributed_assignment(
        &mut self,
        shard_seed: u64,
        context: DistributedContext,
    ) -> Result<()> {
        self.shard_order = assigned_shard_indices(
            self.shard_order.iter().map(|&index| {
                let meta = &self.layout.shards[index];
                (
                    index,
                    ShardId::from_slice(&meta.shard_id).expect("validated shard ID"),
                )
            }),
            shard_seed,
            context,
        )?;
        self.shard_seed = shard_seed;
        self.distributed = Some(context);
        Ok(())
    }

    pub fn new_affinity_assigned(
        store: Store,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        epoch: u64,
        shard_seed: u64,
        affinity: NodeAffinity,
    ) -> Result<Self> {
        Self::new_affinity_assigned_with_incomplete_policy(
            store,
            layout,
            epoch,
            shard_seed,
            affinity,
            IncompleteLayoutPolicy::Reject,
        )
    }

    pub fn new_affinity_assigned_with_incomplete_policy(
        store: Store,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        epoch: u64,
        shard_seed: u64,
        affinity: NodeAffinity,
        incomplete_policy: IncompleteLayoutPolicy,
    ) -> Result<Self> {
        let mut reader = Self::new_with_incomplete_policy(store, layout, incomplete_policy)?;
        reader.apply_affinity_assignment(shard_seed, affinity)?;
        reader.epoch = epoch;
        Ok(reader)
    }

    pub fn new_affinity_assigned_with_registry(
        stores: StoreRegistry,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        epoch: u64,
        shard_seed: u64,
        affinity: NodeAffinity,
    ) -> Result<Self> {
        let mut reader = Self::new_with_registry(stores, layout)?;
        reader.apply_affinity_assignment(shard_seed, affinity)?;
        reader.epoch = epoch;
        Ok(reader)
    }

    fn apply_affinity_assignment(&mut self, shard_seed: u64, affinity: NodeAffinity) -> Result<()> {
        self.shard_order = affinity_assigned_shard_indices(
            self.shard_order.iter().map(|&index| {
                let meta = &self.layout.shards[index];
                (
                    index,
                    ShardId::from_slice(&meta.shard_id).expect("validated shard ID"),
                )
            }),
            shard_seed,
            &affinity,
        )?;
        self.shard_seed = shard_seed;
        self.node_affinity = Some(affinity);
        Ok(())
    }

    #[must_use]
    pub const fn with_epoch_context(mut self, epoch: u64, shard_seed: u64) -> Self {
        self.epoch = epoch;
        self.shard_seed = shard_seed;
        self
    }

    pub fn with_cache(mut self, cache: LocalCache) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn with_decryption_key(
        mut self,
        key_id: &str,
        key: shardpack_format::EncryptionKey,
    ) -> Result<Self> {
        if self
            .layout
            .encryption
            .as_ref()
            .is_none_or(|encryption| encryption.key_id != key_id)
        {
            return Err(Error::DecryptionKeyMismatch);
        }
        self.decryption_key = Some(key);
        Ok(self)
    }

    pub(crate) fn with_cache_max_bytes(mut self, max_bytes: u64) -> Self {
        self.cache = self.cache.map(|cache| cache.with_max_bytes(max_bytes));
        self
    }

    #[must_use]
    pub const fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    pub fn with_network_prefetch(mut self, options: NetworkPrefetch) -> Result<Self> {
        self.prefetch = Some(PrefetchQueue::new(options, self.next_shard_index)?);
        Ok(self)
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: RuntimeMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    #[must_use]
    pub fn metrics(&self) -> RuntimeMetrics {
        self.metrics.clone()
    }

    pub async fn next_record(&mut self) -> Result<Option<EncodedRecord>> {
        if self
            .current
            .as_ref()
            .is_some_and(|reader| reader.buffered_records() != 0)
        {
            return self.next_record_once().await;
        }
        let mut retries = 0_u32;
        let mut backoff = self.retry_policy.initial_backoff;
        loop {
            let retry_position = self.retry_position();
            match self.next_record_once().await {
                Ok(record) => return Ok(record),
                Err(error)
                    if is_retryable(&error, self.cache.is_some())
                        && retries < self.retry_policy.max_retries =>
                {
                    retries += 1;
                    self.metrics.retry(is_checksum_failure(&error));
                    self.invalidate_failed_cache_entry().await?;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(self.retry_policy.max_backoff);
                    let checkpoint = self.checkpoint_from_retry_position(retry_position);
                    self.restore_checkpoint(&checkpoint).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn invalidate_failed_cache_entry(&mut self) -> Result<()> {
        let Some(cache) = &self.cache else {
            return Ok(());
        };
        let meta = self
            .current_shard_index
            .map(|index| &self.layout.shards[index])
            .or_else(|| {
                self.shard_order
                    .get(self.next_shard_index)
                    .map(|&index| &self.layout.shards[index])
            });
        let Some(meta) = meta else {
            return Ok(());
        };
        let id =
            ShardId::from_slice(&meta.shard_id).map_err(|source| Error::Manifest { source })?;
        cache
            .invalidate(id)
            .await
            .map_err(|source| Error::Cache { source })?;
        Ok(())
    }

    async fn next_record_once(&mut self) -> Result<Option<EncodedRecord>> {
        loop {
            if let Some(reader) = &mut self.current {
                if let Some(record) = reader
                    .next_record()
                    .await
                    .map_err(|source| Error::Format { source })?
                {
                    let record = EncodedRecord {
                        sample_id: record.sample_id,
                        flags: record.flags,
                        payload: record.payload,
                    };
                    self.metrics.record_read(record.payload.len());
                    return Ok(Some(record));
                }
                self.validate_finished_shard()?;
                self.current = None;
                self.current_shard_index = None;
                self.records_before_current_reader = 0;
                self.verify_current_fingerprint = true;
            }

            let Some(shard_index) = self.shard_order.get(self.next_shard_index).copied() else {
                return Ok(None);
            };
            let meta = &self.layout.shards[shard_index];
            let object = if let Some(prefetch) = &mut self.prefetch {
                let object = prefetch
                    .take_next(
                        self.next_shard_index,
                        &self.stores,
                        self.cache.as_ref(),
                        &self.layout.shards,
                        &self.shard_order,
                    )?
                    .expect("layout has a shard at next index");
                self.metrics.prefetched_shard();
                object
            } else {
                open_object(&self.stores, self.cache.as_ref(), meta).await?
            };
            self.next_shard_index += 1;
            let plaintext = object.plaintext;
            let reader = AsyncShardReader::with_limits_and_key(
                Blake3Reader::new(object.reader),
                ReaderLimits::default(),
                if plaintext {
                    None
                } else {
                    self.decryption_key.clone()
                },
            )
            .await
            .map_err(|source| Error::Format { source })?
            .without_content_fingerprint();
            self.verify_current_fingerprint = !plaintext;
            self.current_shard_index = Some(shard_index);
            self.current = Some(reader);
        }
    }

    async fn restore_checkpoint(&mut self, checkpoint: &LoaderCheckpoint) -> Result<()> {
        let cache = self.cache.clone();
        let retry_policy = self.retry_policy;
        let metrics = self.metrics.clone();
        let decryption_key = self.decryption_key.clone();
        let prefetch_options = self.prefetch.as_ref().map(PrefetchQueue::options);
        let policy = if proto::LayoutState::try_from(self.layout.state)
            == Ok(proto::LayoutState::Incomplete)
        {
            IncompleteLayoutPolicy::Allow
        } else {
            IncompleteLayoutPolicy::Reject
        };
        let mut restored = Self::resume_with_incomplete_policy_and_key(
            self.stores.clone(),
            Arc::clone(&self.layout),
            checkpoint,
            policy,
            decryption_key,
            cache.clone(),
        )
        .await?;
        restored.retry_policy = retry_policy;
        restored.metrics = metrics;
        if let Some(options) = prefetch_options {
            restored.prefetch = Some(PrefetchQueue::new(options, restored.next_shard_index)?);
        }
        *self = restored;
        Ok(())
    }

    /// Restores a loader from a block-boundary checkpoint. The containing
    /// block is fetched again and CRC-checked, then already-consumed records
    /// in that block are discarded.
    pub async fn resume(
        store: Store,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        checkpoint: &LoaderCheckpoint,
    ) -> Result<Self> {
        Self::resume_with_incomplete_policy(
            store,
            layout,
            checkpoint,
            IncompleteLayoutPolicy::Reject,
        )
        .await
    }

    pub async fn resume_with_incomplete_policy(
        store: Store,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        checkpoint: &LoaderCheckpoint,
        incomplete_policy: IncompleteLayoutPolicy,
    ) -> Result<Self> {
        let layout = layout.into();
        shardpack_manifest::validate_layout(&layout)
            .map_err(|source| Error::Manifest { source })?;
        let stores = registry_for_single_store(store, &layout)?;
        Self::resume_with_incomplete_policy_and_key(
            stores,
            layout,
            checkpoint,
            incomplete_policy,
            None,
            None,
        )
        .await
    }

    /// Restores an unencrypted loader whose layout may reference multiple
    /// named stores.
    pub async fn resume_with_registry(
        stores: StoreRegistry,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        checkpoint: &LoaderCheckpoint,
    ) -> Result<Self> {
        Self::resume_with_incomplete_policy_and_key(
            stores,
            layout,
            checkpoint,
            IncompleteLayoutPolicy::Reject,
            None,
            None,
        )
        .await
    }

    /// Restores an unencrypted loader and uses `cache` for the current shard.
    /// The cached stream is consumed from byte zero to preserve a complete
    /// local shard while decoding resumes at the recorded block boundary.
    pub async fn resume_with_registry_and_cache(
        stores: StoreRegistry,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        checkpoint: &LoaderCheckpoint,
        cache: LocalCache,
    ) -> Result<Self> {
        Self::resume_with_incomplete_policy_and_key(
            stores,
            layout,
            checkpoint,
            IncompleteLayoutPolicy::Reject,
            None,
            Some(cache),
        )
        .await
    }

    pub async fn resume_encrypted(
        store: Store,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        checkpoint: &LoaderCheckpoint,
        key_id: &str,
        key: shardpack_format::EncryptionKey,
    ) -> Result<Self> {
        let layout = layout.into();
        if layout
            .encryption
            .as_ref()
            .is_none_or(|encryption| encryption.key_id != key_id)
        {
            return Err(Error::DecryptionKeyMismatch);
        }
        shardpack_manifest::validate_layout(&layout)
            .map_err(|source| Error::Manifest { source })?;
        let stores = registry_for_single_store(store, &layout)?;
        Self::resume_with_incomplete_policy_and_key(
            stores,
            layout,
            checkpoint,
            IncompleteLayoutPolicy::Reject,
            Some(key),
            None,
        )
        .await
    }

    pub async fn resume_encrypted_with_registry(
        stores: StoreRegistry,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        checkpoint: &LoaderCheckpoint,
        key_id: &str,
        key: shardpack_format::EncryptionKey,
    ) -> Result<Self> {
        let layout = layout.into();
        if layout
            .encryption
            .as_ref()
            .is_none_or(|encryption| encryption.key_id != key_id)
        {
            return Err(Error::DecryptionKeyMismatch);
        }
        Self::resume_with_incomplete_policy_and_key(
            stores,
            layout,
            checkpoint,
            IncompleteLayoutPolicy::Reject,
            Some(key),
            None,
        )
        .await
    }

    pub async fn resume_encrypted_with_registry_and_cache(
        stores: StoreRegistry,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        checkpoint: &LoaderCheckpoint,
        key_id: &str,
        key: shardpack_format::EncryptionKey,
        cache: LocalCache,
    ) -> Result<Self> {
        let layout = layout.into();
        if layout
            .encryption
            .as_ref()
            .is_none_or(|encryption| encryption.key_id != key_id)
        {
            return Err(Error::DecryptionKeyMismatch);
        }
        Self::resume_with_incomplete_policy_and_key(
            stores,
            layout,
            checkpoint,
            IncompleteLayoutPolicy::Reject,
            Some(key),
            Some(cache),
        )
        .await
    }

    async fn resume_with_incomplete_policy_and_key(
        stores: StoreRegistry,
        layout: impl Into<Arc<proto::LayoutManifest>>,
        checkpoint: &LoaderCheckpoint,
        incomplete_policy: IncompleteLayoutPolicy,
        decryption_key: Option<shardpack_format::EncryptionKey>,
        cache: Option<LocalCache>,
    ) -> Result<Self> {
        let mut result =
            Self::new_with_registry_and_incomplete_policy(stores, layout, incomplete_policy)?;
        if let Some(affinity) = checkpoint.node_affinity.clone() {
            result.apply_affinity_assignment(checkpoint.shard_seed, affinity)?;
        } else if let Some(context) = checkpoint.distributed {
            result.apply_distributed_assignment(checkpoint.shard_seed, context)?;
        }
        if result.layout.snapshot_id.as_slice() != checkpoint.snapshot_id {
            return Err(Error::InvalidCheckpoint {
                reason: "snapshot ID does not match layout",
            });
        }
        if result.layout.layout_id.as_slice() != checkpoint.layout_id {
            return Err(Error::InvalidCheckpoint {
                reason: "layout ID does not match layout",
            });
        }
        result.epoch = checkpoint.epoch;
        result.shard_seed = checkpoint.shard_seed;
        result.decryption_key = decryption_key.clone();
        result.cache = cache;
        result.next_shard_index = checkpoint.next_shard_index;

        let Some(current_id) = checkpoint.current_shard_id else {
            if checkpoint.records_yielded_in_current_shard != 0
                || checkpoint.block_index != 0
                || checkpoint.block_start_offset != 0
                || checkpoint.record_index != 0
            {
                return Err(Error::InvalidCheckpoint {
                    reason: "between-shard checkpoint contains block coordinates",
                });
            }
            if result.next_shard_index > result.shard_order.len() {
                return Err(Error::InvalidCheckpoint {
                    reason: "next shard index is out of bounds",
                });
            }
            return Ok(result);
        };

        let current_index =
            checkpoint
                .next_shard_index
                .checked_sub(1)
                .ok_or(Error::InvalidCheckpoint {
                    reason: "current shard requires a nonzero next shard index",
                })?;
        let shard_index =
            result
                .shard_order
                .get(current_index)
                .copied()
                .ok_or(Error::InvalidCheckpoint {
                    reason: "current shard index is out of bounds",
                })?;
        let meta = &result.layout.shards[shard_index];
        if meta.shard_id.as_slice() != current_id {
            return Err(Error::InvalidCheckpoint {
                reason: "current shard ID does not match layout",
            });
        }
        if checkpoint.block_start_offset < shardpack_format::SHARD_HEADER_SIZE as u64
            || checkpoint.block_start_offset >= meta.byte_size
            || checkpoint.records_yielded_in_current_shard < u64::from(checkpoint.record_index)
        {
            return Err(Error::InvalidCheckpoint {
                reason: "invalid block offset or record count",
            });
        }
        let (object, verify_full_identity, plaintext) = if result.cache.is_some() {
            let object = open_object(&result.stores, result.cache.as_ref(), meta).await?;
            let plaintext = object.plaintext;
            let mut object = Blake3Reader::new(object.reader);
            discard_exact(&mut object, checkpoint.block_start_offset).await?;
            (object, !plaintext, plaintext)
        } else {
            let object = result
                .stores
                .get(&shardpack_manifest::object_location(meta).store_id)
                .map_err(|source| Error::Storage { source })?
                .open_reader_from(
                    &shardpack_manifest::object_location(meta).object_key,
                    checkpoint.block_start_offset,
                )
                .await
                .map_err(|source| Error::Storage { source })?;
            (Blake3Reader::new(object), false, false)
        };
        let mut reader = AsyncShardReader::from_block_boundary_with_key(
            object,
            ReaderLimits::default(),
            checkpoint.block_index,
            checkpoint.block_start_offset,
            if plaintext { None } else { decryption_key },
        )
        .without_content_fingerprint();
        reader
            .skip_records(checkpoint.record_index)
            .await
            .map_err(|source| Error::Format { source })?;
        result.records_before_current_reader =
            checkpoint.records_yielded_in_current_shard - u64::from(checkpoint.record_index);
        result.verify_current_fingerprint = verify_full_identity;
        result.current_shard_index = Some(shard_index);
        result.current = Some(reader);
        Ok(result)
    }

    #[must_use]
    pub fn position(&self) -> ReaderPosition {
        ReaderPosition {
            next_shard_index: self.next_shard_index,
            current_shard_id: self.current_shard_index.map(|index| {
                let meta = &self.layout.shards[index];
                meta.shard_id
                    .as_slice()
                    .try_into()
                    .expect("validated shard ID")
            }),
            records_yielded_in_current_shard: self.current.as_ref().map_or(0, |reader| {
                self.records_before_current_reader + reader.records_yielded()
            }),
        }
    }

    #[must_use]
    pub fn checkpoint(&self) -> LoaderCheckpoint {
        self.checkpoint_from_retry_position(self.retry_position())
    }

    fn retry_position(&self) -> RetryPosition {
        let reader = self
            .current
            .as_ref()
            .map_or(default_reader_position(), AsyncShardReader::position);
        RetryPosition {
            next_shard_index: self.next_shard_index,
            current_shard_id: self.current_shard_index.map(|index| {
                self.layout.shards[index]
                    .shard_id
                    .as_slice()
                    .try_into()
                    .expect("validated shard ID")
            }),
            records_yielded_in_current_shard: self.current.as_ref().map_or(0, |reader| {
                self.records_before_current_reader + reader.records_yielded()
            }),
            reader,
        }
    }

    fn checkpoint_from_retry_position(&self, position: RetryPosition) -> LoaderCheckpoint {
        LoaderCheckpoint {
            snapshot_id: self
                .layout
                .snapshot_id
                .as_slice()
                .try_into()
                .expect("validated snapshot ID"),
            layout_id: self
                .layout
                .layout_id
                .as_slice()
                .try_into()
                .expect("validated layout ID"),
            epoch: self.epoch,
            shard_seed: self.shard_seed,
            next_shard_index: position.next_shard_index,
            current_shard_id: position.current_shard_id,
            records_yielded_in_current_shard: position.records_yielded_in_current_shard,
            block_index: position.reader.block_index,
            block_start_offset: position.reader.block_start_offset,
            record_index: position.reader.record_index,
            distributed: self.distributed,
            node_affinity: self.node_affinity.clone(),
        }
    }

    fn validate_finished_shard(&self) -> Result<()> {
        let reader = self.current.as_ref().expect("reader exists");
        let meta = &self.layout.shards[self.current_shard_index.expect("metadata exists")];
        if self.records_before_current_reader + reader.records_yielded() != meta.record_count {
            return Err(mismatch(meta, "record count differs"));
        }
        if reader.block_count() != meta.block_count {
            return Err(mismatch(meta, "block count differs"));
        }
        if self.verify_current_fingerprint
            && (reader.input_ref().bytes_read() != meta.byte_size
                || reader.input_ref().digest().as_slice() != meta.shard_id)
        {
            return Err(mismatch(meta, "exact shard identity differs"));
        }
        self.metrics
            .shard_completed(meta.byte_size, self.verify_current_fingerprint);
        Ok(())
    }
}

async fn discard_exact<R: AsyncRead + Unpin>(reader: &mut R, mut bytes: u64) -> Result<()> {
    // Keep the async state machine small; embedding a large array here is
    // multiplied through callers that contain this future.
    let mut buffer = vec![0_u8; 64 * 1024];
    while bytes != 0 {
        let chunk = usize::try_from(bytes.min(buffer.len() as u64)).expect("bounded by buffer");
        reader
            .read_exact(&mut buffer[..chunk])
            .await
            .map_err(|source| Error::Format {
                source: shardpack_format::Error::Io { source },
            })?;
        bytes -= chunk as u64;
    }
    Ok(())
}

async fn open_object(
    stores: &StoreRegistry,
    cache: Option<&LocalCache>,
    meta: &proto::ShardMeta,
) -> Result<PrefetchedObject> {
    let location = shardpack_manifest::object_location(meta);
    let store = stores
        .get(&location.store_id)
        .map_err(|source| Error::Storage { source })?;
    if let Some(cache) = cache {
        let id =
            ShardId::from_slice(&meta.shard_id).map_err(|source| Error::Manifest { source })?;
        let object = cache
            .open_object(store, &location.object_key, id, meta.byte_size)
            .await
            .map_err(|source| Error::Cache { source })?;
        Ok(PrefetchedObject {
            reader: object.reader,
            plaintext: object.plaintext,
        })
    } else {
        let reader = store
            .open_reader(&location.object_key)
            .await
            .map_err(|source| Error::Storage { source })?;
        Ok(PrefetchedObject {
            reader,
            plaintext: false,
        })
    }
}

fn registry_for_single_store(
    store: Store,
    layout: &proto::LayoutManifest,
) -> Result<StoreRegistry> {
    let mut stores = StoreRegistry::new();
    for shard in &layout.shards {
        let store_id = &shardpack_manifest::object_location(shard).store_id;
        if !stores.contains(store_id) {
            stores
                .insert(store_id, store.clone())
                .map_err(|source| Error::Storage { source })?;
        }
    }
    Ok(stores)
}

const fn default_reader_position() -> AsyncReaderPosition {
    AsyncReaderPosition {
        block_index: 0,
        block_start_offset: 0,
        record_index: 0,
        next_block_offset: 0,
    }
}

fn mismatch(meta: &proto::ShardMeta, reason: &'static str) -> Error {
    Error::ShardMismatch {
        shard: hex::encode(&meta.shard_id),
        reason,
    }
}

fn is_retryable(error: &Error, cache_enabled: bool) -> bool {
    match error {
        Error::Format { source } => matches!(
            source,
            shardpack_format::Error::Io { .. }
                | shardpack_format::Error::TruncatedBlock
                | shardpack_format::Error::HeaderChecksumMismatch
                | shardpack_format::Error::PayloadChecksumMismatch
        ),
        Error::Storage { source } => !matches!(
            source,
            shardpack_storage::Error::InvalidKey { .. }
                | shardpack_storage::Error::NotFound { .. }
                | shardpack_storage::Error::AlreadyExists { .. }
                | shardpack_storage::Error::ImmutableWriteUnsupported { .. }
        ),
        Error::Cache { source } => matches!(
            source,
            shardpack_cache::Error::Storage { .. } | shardpack_cache::Error::Io { .. }
        ),
        Error::ShardMismatch { reason, .. } => {
            cache_enabled && *reason == "exact shard identity differs"
        }
        _ => false,
    }
}

fn is_checksum_failure(error: &Error) -> bool {
    matches!(
        error,
        Error::Format {
            source: shardpack_format::Error::HeaderChecksumMismatch
                | shardpack_format::Error::PayloadChecksumMismatch
        }
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures_util::stream;
    use shardpack_format::{BlockCodec, ContentFingerprint, ShardWriter, WriterOptions};
    use shardpack_manifest::{layout_id, proto};

    use super::{DatasetReader, IncompleteLayoutPolicy, RetryPolicy};
    use crate::NodeAffinity;
    use crate::{DistributedContext, Error, NetworkPrefetch, RuntimeMetrics};

    fn proto_fingerprint(value: ContentFingerprint) -> proto::ContentFingerprint {
        proto::ContentFingerprint {
            record_count: value.record_count,
            accumulator_a: value.accumulator_a.to_vec(),
            accumulator_b: value.accumulator_b.to_vec(),
        }
    }

    #[tokio::test]
    async fn streams_records_directly_from_opendal() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([1; 16], 3, b"one").unwrap();
        writer.add_record([2; 16], 4, b"two").unwrap();
        let (bytes, summary) = writer.finish().unwrap();
        let object_uri = "shards/test.spk";
        store
            .write_immutable(
                object_uri,
                Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) })),
            )
            .await
            .unwrap();
        let mut layout = proto::LayoutManifest {
            snapshot_id: vec![9; 32],
            builder_version: "test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1024,
                target_block_bytes: 1024,
                block_codec: "none".into(),
                shuffle_seed: None,
                max_shard_bytes: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            total_records: summary.record_count,
            total_bytes: summary.byte_size,
            content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            shards: vec![proto::ShardMeta {
                shard_id: summary.shard_id.to_vec(),
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: object_uri.into(),
                }),
                byte_size: summary.byte_size,
                record_count: summary.record_count,
                block_count: summary.block_count,
                content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            }],
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();
        let mut incomplete = layout.clone();
        incomplete.state = proto::LayoutState::Incomplete as i32;
        incomplete.materialization_stats = Some(proto::MaterializationStats {
            expected_records: 3,
            written_records: 2,
            skipped_records: 1,
        });
        incomplete.layout_id = layout_id(&incomplete).0.to_vec();
        assert!(matches!(
            DatasetReader::new(store.clone(), incomplete.clone()),
            Err(Error::IncompleteLayout)
        ));
        DatasetReader::new_with_incomplete_policy(
            store.clone(),
            incomplete,
            IncompleteLayoutPolicy::Allow,
        )
        .unwrap();
        let metrics = RuntimeMetrics::default();
        let layout = Arc::new(layout);
        let mut reader = DatasetReader::new(store, layout.clone())
            .unwrap()
            .with_metrics(metrics.clone());
        assert!(Arc::ptr_eq(&reader.layout, &layout));
        assert_eq!(reader.next_record().await.unwrap().unwrap().payload, "one");
        assert_eq!(reader.next_record().await.unwrap().unwrap().payload, "two");
        assert!(reader.next_record().await.unwrap().is_none());
        assert_eq!(reader.position().next_shard_index, 1);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.records_read, 2);
        assert_eq!(snapshot.encoded_bytes_read, 6);
        assert_eq!(snapshot.shards_completed, 1);
        assert_eq!(snapshot.shard_bytes_completed, summary.byte_size);
        assert_eq!(snapshot.shards_verified, 1);
        assert_eq!(snapshot.shard_bytes_verified, summary.byte_size);
    }

    #[tokio::test]
    async fn streams_one_layout_across_multiple_stores() {
        let first_root = tempfile::tempdir().unwrap();
        let second_root = tempfile::tempdir().unwrap();
        let first = shardpack_storage::Store::filesystem(first_root.path()).unwrap();
        let second = shardpack_storage::Store::filesystem(second_root.path()).unwrap();
        let mut shards = Vec::new();
        let mut aggregate = ContentFingerprint::default();
        let mut total_bytes = 0;

        for (store_id, store, value) in [("hot", &first, 1_u8), ("archive", &second, 2_u8)] {
            let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
            writer.add_record([value; 16], 0, &[value]).unwrap();
            let (bytes, summary) = writer.finish().unwrap();
            let object_key = format!("shards/{value}.spk");
            store
                .write_immutable(
                    &object_key,
                    Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) })),
                )
                .await
                .unwrap();
            aggregate.merge(&summary.content_fingerprint);
            total_bytes += summary.byte_size;
            shards.push(proto::ShardMeta {
                shard_id: summary.shard_id.to_vec(),
                location: Some(proto::ObjectLocation {
                    store_id: store_id.into(),
                    object_key,
                }),
                byte_size: summary.byte_size,
                record_count: summary.record_count,
                block_count: summary.block_count,
                content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            });
        }

        let mut layout = proto::LayoutManifest {
            snapshot_id: vec![7; 32],
            builder_version: "multi-store-test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1024,
                target_block_bytes: 1024,
                block_codec: "none".into(),
                shuffle_seed: None,
                max_shard_bytes: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            total_records: aggregate.record_count,
            total_bytes,
            content_fingerprint: Some(proto_fingerprint(aggregate)),
            shards,
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();

        let mut stores = shardpack_storage::StoreRegistry::new();
        stores.insert("hot", first).unwrap();
        stores.insert("archive", second).unwrap();
        let mut reader = DatasetReader::new_with_registry(stores, layout).unwrap();
        assert_eq!(
            reader
                .next_record()
                .await
                .unwrap()
                .unwrap()
                .payload
                .as_ref(),
            [1]
        );
        assert_eq!(
            reader
                .next_record()
                .await
                .unwrap()
                .unwrap()
                .payload
                .as_ref(),
            [2]
        );
        assert!(reader.next_record().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn streams_encrypted_layout_with_matching_external_key() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let key = shardpack_format::EncryptionKey::new([0x71; 32]);
        let mut writer =
            ShardWriter::new_encrypted(Vec::new(), WriterOptions::default(), key.clone()).unwrap();
        writer.add_record([7; 16], 0, b"private").unwrap();
        writer.add_record([8; 16], 0, b"second").unwrap();
        let (bytes, summary) = writer.finish().unwrap();
        let object_uri = "shards/encrypted.spk";
        store
            .write_immutable(
                object_uri,
                Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) })),
            )
            .await
            .unwrap();
        let mut layout = proto::LayoutManifest {
            snapshot_id: vec![7; 32],
            builder_version: "encrypted-test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1024,
                target_block_bytes: 1024,
                block_codec: "none".into(),
                shuffle_seed: None,
                max_shard_bytes: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            total_records: 2,
            total_bytes: summary.byte_size,
            content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            shards: vec![proto::ShardMeta {
                shard_id: summary.shard_id.to_vec(),
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: object_uri.into(),
                }),
                byte_size: summary.byte_size,
                record_count: 2,
                block_count: 1,
                content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            }],
            state: proto::LayoutState::Complete as i32,
            encryption: Some(proto::EncryptionDescriptor {
                algorithm: proto::EncryptionAlgorithm::Aes256Gcm as i32,
                key_id: "kms://training/key-7".into(),
            }),
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();

        assert!(matches!(
            DatasetReader::new(store.clone(), layout.clone())
                .unwrap()
                .next_record()
                .await,
            Err(Error::Format {
                source: shardpack_format::Error::MissingDecryptionKey
            })
        ));
        assert!(matches!(
            DatasetReader::new(store.clone(), layout.clone())
                .unwrap()
                .with_decryption_key("wrong", key.clone()),
            Err(Error::DecryptionKeyMismatch)
        ));
        let mut reader = DatasetReader::new(store, layout)
            .unwrap()
            .with_decryption_key("kms://training/key-7", key)
            .unwrap();
        assert_eq!(
            reader.next_record().await.unwrap().unwrap().payload,
            "private"
        );
        let checkpoint = reader.checkpoint();
        let mut resumed = DatasetReader::resume_encrypted_with_registry(
            reader.stores.clone(),
            (*reader.layout).clone(),
            &checkpoint,
            "kms://training/key-7",
            shardpack_format::EncryptionKey::new([0x71; 32]),
        )
        .await
        .unwrap();
        assert_eq!(
            resumed.next_record().await.unwrap().unwrap().payload,
            "second"
        );
        assert!(resumed.next_record().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn encrypted_layout_builds_plaintext_cache_during_first_epoch() {
        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let key = shardpack_format::EncryptionKey::new([0x33; 32]);
        let mut writer = ShardWriter::new_encrypted(
            Vec::new(),
            WriterOptions {
                target_block_bytes: 54,
                ..WriterOptions::default()
            },
            key.clone(),
        )
        .unwrap();
        for value in 1..=5_u8 {
            writer.add_record([value; 16], 0, &[value; 3]).unwrap();
        }
        let (bytes, summary) = writer.finish().unwrap();
        assert!(summary.block_count > 1);
        let object_key = "shards/public-encrypted.spk";
        store
            .write_bytes_immutable(object_key, Bytes::from(bytes))
            .await
            .unwrap();
        let mut layout = proto::LayoutManifest {
            snapshot_id: vec![3; 32],
            builder_version: "plaintext-cache-test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1024,
                target_block_bytes: 54,
                block_codec: "none".into(),
                shuffle_seed: None,
                max_shard_bytes: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            total_records: summary.record_count,
            total_bytes: summary.byte_size,
            content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            shards: vec![proto::ShardMeta {
                shard_id: summary.shard_id.to_vec(),
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: object_key.into(),
                }),
                byte_size: summary.byte_size,
                record_count: summary.record_count,
                block_count: summary.block_count,
                content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            }],
            state: proto::LayoutState::Complete as i32,
            encryption: Some(proto::EncryptionDescriptor {
                algorithm: proto::EncryptionAlgorithm::Aes256Gcm as i32,
                key_id: "test-key".into(),
            }),
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();
        let cache = shardpack_cache::LocalCache::new(cache_root.path()).with_plaintext(key.clone());

        let mut cold = DatasetReader::new(store.clone(), layout.clone())
            .unwrap()
            .with_cache(cache.clone())
            .with_decryption_key("test-key", key.clone())
            .unwrap();
        let mut cold_records = vec![cold.next_record().await.unwrap().unwrap().sample_id[0]];
        let checkpoint = cold.checkpoint();
        let stores = cold.stores.clone();
        drop(cold);
        let mut resumed = DatasetReader::resume_encrypted_with_registry_and_cache(
            stores,
            layout.clone(),
            &checkpoint,
            "test-key",
            key.clone(),
            cache.clone(),
        )
        .await
        .unwrap();
        while let Some(record) = resumed.next_record().await.unwrap() {
            cold_records.push(record.sample_id[0]);
        }
        assert_eq!(cold_records, vec![1, 2, 3, 4, 5]);
        // The abandoned partial from the interrupted first pass is never
        // published; resume safely rebuilds it from the encrypted source.
        assert_eq!(cache.metrics().snapshot().misses, 2);

        store.delete(object_key).await.unwrap();
        let mut hot = DatasetReader::new(store, layout)
            .unwrap()
            .with_cache(cache.clone())
            .with_decryption_key("test-key", key)
            .unwrap();
        let mut hot_records = Vec::new();
        while let Some(record) = hot.next_record().await.unwrap() {
            hot_records.push(record.sample_id[0]);
        }
        assert_eq!(hot_records, cold_records);
        assert_eq!(cache.metrics().snapshot().hits, 1);
    }

    #[tokio::test]
    async fn prefetches_multiple_shards_with_bounded_ordered_streams() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let mut shards = Vec::new();
        let mut aggregate = ContentFingerprint::default();
        let mut total_bytes = 0_u64;
        for value in 1..=4_u8 {
            let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
            writer
                .add_record([value; 16], u32::from(value), &vec![value; 257])
                .unwrap();
            let (bytes, summary) = writer.finish().unwrap();
            let object_uri = format!("shards/prefetch-{value}.spk");
            store
                .write_immutable(
                    &object_uri,
                    Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) })),
                )
                .await
                .unwrap();
            aggregate.merge(&summary.content_fingerprint);
            total_bytes += summary.byte_size;
            shards.push(proto::ShardMeta {
                shard_id: summary.shard_id.to_vec(),
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: object_uri,
                }),
                byte_size: summary.byte_size,
                record_count: summary.record_count,
                block_count: summary.block_count,
                content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            });
        }
        let mut layout = proto::LayoutManifest {
            snapshot_id: vec![8; 32],
            builder_version: "prefetch-test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1024,
                target_block_bytes: 1024,
                block_codec: "none".into(),
                shuffle_seed: None,
                max_shard_bytes: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            total_records: aggregate.record_count,
            total_bytes,
            content_fingerprint: Some(proto_fingerprint(aggregate)),
            shards,
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();
        let contexts = [
            DistributedContext {
                rank: 0,
                world_size: 2,
                worker: 0,
                workers_per_rank: 1,
            },
            DistributedContext {
                rank: 1,
                world_size: 2,
                worker: 0,
                workers_per_rank: 1,
            },
        ];
        let mut assigned_outputs = Vec::new();
        for context in contexts {
            let mut assigned =
                DatasetReader::new_assigned(store.clone(), layout.clone(), 5, 77, context).unwrap();
            let first = assigned.next_record().await.unwrap();
            if context.rank == 0 {
                let checkpoint = assigned.checkpoint();
                assert_eq!(checkpoint.distributed, Some(context));
                let mut resumed = DatasetReader::resume(store.clone(), layout.clone(), &checkpoint)
                    .await
                    .unwrap();
                if let Some(record) = first {
                    assigned_outputs.push(record.sample_id[0]);
                }
                while let Some(record) = resumed.next_record().await.unwrap() {
                    assigned_outputs.push(record.sample_id[0]);
                }
            } else {
                if let Some(record) = first {
                    assigned_outputs.push(record.sample_id[0]);
                }
                while let Some(record) = assigned.next_record().await.unwrap() {
                    assigned_outputs.push(record.sample_id[0]);
                }
            }
        }
        assigned_outputs.sort_unstable();
        assert_eq!(assigned_outputs, vec![1, 2, 3, 4]);

        let nodes = vec!["gpu-a".to_owned(), "gpu-b".to_owned()];
        let mut affinity_outputs = Vec::new();
        for node_id in &nodes {
            let affinity = NodeAffinity {
                node_id: node_id.clone(),
                node_ids: nodes.clone(),
                local_rank: 0,
                local_world_size: 1,
                worker: 0,
                workers_per_rank: 1,
                affinity_seed: 123,
            };
            let mut assigned = DatasetReader::new_affinity_assigned(
                store.clone(),
                layout.clone(),
                5,
                77,
                affinity.clone(),
            )
            .unwrap();
            let first = assigned.next_record().await.unwrap();
            let checkpoint = assigned.checkpoint();
            assert_eq!(checkpoint.node_affinity, Some(affinity));
            let mut resumed = DatasetReader::resume(store.clone(), layout.clone(), &checkpoint)
                .await
                .unwrap();
            if let Some(record) = first {
                affinity_outputs.push(record.sample_id[0]);
            }
            while let Some(record) = resumed.next_record().await.unwrap() {
                affinity_outputs.push(record.sample_id[0]);
            }
        }
        affinity_outputs.sort_unstable();
        assert_eq!(affinity_outputs, vec![1, 2, 3, 4]);

        let metrics = RuntimeMetrics::default();
        let mut reader = DatasetReader::new(store, layout)
            .unwrap()
            .with_metrics(metrics.clone())
            .with_network_prefetch(NetworkPrefetch {
                concurrency: 3,
                max_bytes: 192,
            })
            .unwrap();
        let mut observed = Vec::new();
        while let Some(record) = reader.next_record().await.unwrap() {
            observed.push(record.sample_id[0]);
        }
        assert_eq!(observed, vec![1, 2, 3, 4]);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.prefetched_shards, 4);
        assert_eq!(snapshot.records_read, 4);
    }

    #[tokio::test]
    async fn resumes_from_the_current_block_without_loss_or_duplicates() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let mut writer = ShardWriter::new(
            Vec::new(),
            WriterOptions {
                target_block_bytes: 54,
                codec: BlockCodec::None,
                zstd_level: 3,
            },
        )
        .unwrap();
        for value in 1..=5_u8 {
            writer.add_record([value; 16], 0, &[value; 3]).unwrap();
        }
        let (bytes, summary) = writer.finish().unwrap();
        assert_eq!(summary.block_count, 3);
        let object_uri = "shards/resume.spk";
        store
            .write_immutable(
                object_uri,
                Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) })),
            )
            .await
            .unwrap();
        let mut layout = proto::LayoutManifest {
            snapshot_id: vec![7; 32],
            builder_version: "test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1024,
                target_block_bytes: 54,
                block_codec: "none".into(),
                shuffle_seed: None,
                max_shard_bytes: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            total_records: summary.record_count,
            total_bytes: summary.byte_size,
            content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            shards: vec![proto::ShardMeta {
                shard_id: summary.shard_id.to_vec(),
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: object_uri.into(),
                }),
                byte_size: summary.byte_size,
                record_count: summary.record_count,
                block_count: summary.block_count,
                content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            }],
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();

        let mut first = DatasetReader::new(store.clone(), layout.clone())
            .unwrap()
            .with_epoch_context(4, 99);
        assert_eq!(
            first.next_record().await.unwrap().unwrap().sample_id,
            [1; 16]
        );
        let checkpoint = first.checkpoint();
        assert_eq!(checkpoint.block_index, 0);
        assert_eq!(checkpoint.record_index, 1);
        assert_eq!(checkpoint.epoch, 4);
        assert_eq!(
            first.next_record().await.unwrap().unwrap().sample_id,
            [2; 16]
        );
        assert_eq!(
            first.next_record().await.unwrap().unwrap().sample_id,
            [3; 16]
        );
        let later_checkpoint = first.checkpoint();
        assert_eq!(later_checkpoint.block_index, 1);
        assert!(later_checkpoint.block_start_offset > checkpoint.block_start_offset);

        let mut invalid_cases = Vec::new();
        let mut wrong_snapshot = checkpoint.clone();
        wrong_snapshot.snapshot_id = [0xff; 32];
        invalid_cases.push(wrong_snapshot);
        let mut wrong_layout = checkpoint.clone();
        wrong_layout.layout_id = [0xfe; 32];
        invalid_cases.push(wrong_layout);
        let mut wrong_shard = checkpoint.clone();
        wrong_shard.current_shard_id = Some([0xfd; 32]);
        invalid_cases.push(wrong_shard);
        let mut invalid_offset = checkpoint.clone();
        invalid_offset.block_start_offset = 1;
        invalid_cases.push(invalid_offset);
        let mut invalid_record_index = checkpoint.clone();
        invalid_record_index.record_index = 100;
        invalid_cases.push(invalid_record_index);
        for invalid in invalid_cases {
            assert!(matches!(
                DatasetReader::resume(store.clone(), layout.clone(), &invalid).await,
                Err(Error::InvalidCheckpoint { .. })
            ));
        }

        let mut between_shards = checkpoint.clone();
        between_shards.current_shard_id = None;
        assert!(matches!(
            DatasetReader::resume(store.clone(), layout.clone(), &between_shards).await,
            Err(Error::InvalidCheckpoint { .. })
        ));

        let resumed_metrics = RuntimeMetrics::default();
        let mut resumed = DatasetReader::resume(store.clone(), layout.clone(), &checkpoint)
            .await
            .unwrap()
            .with_metrics(resumed_metrics.clone());
        let mut remaining = Vec::new();
        while let Some(record) = resumed.next_record().await.unwrap() {
            remaining.push(record.sample_id[0]);
        }
        assert_eq!(remaining, vec![2, 3, 4, 5]);
        let snapshot = resumed_metrics.snapshot();
        assert_eq!(snapshot.shards_completed, 1);
        assert_eq!(snapshot.shards_verified, 0);

        let mut wrong_identity_layout = layout.clone();
        wrong_identity_layout.shards[0].shard_id[0] ^= 0x80;
        wrong_identity_layout.layout_id = layout_id(&wrong_identity_layout).0.to_vec();
        let metrics = RuntimeMetrics::default();
        let mut wrong_identity = DatasetReader::new(store.clone(), wrong_identity_layout)
            .unwrap()
            .with_metrics(metrics.clone());
        for _ in 0..summary.record_count {
            assert!(wrong_identity.next_record().await.unwrap().is_some());
        }
        assert!(matches!(
            wrong_identity.next_record().await,
            Err(Error::ShardMismatch {
                reason: "exact shard identity differs",
                ..
            })
        ));
        assert_eq!(metrics.snapshot().retries, 0);

        let cache_root = tempfile::tempdir().unwrap();
        let cache = shardpack_cache::LocalCache::new(cache_root.path());
        let shard_id = shardpack_manifest::ShardId(summary.shard_id);
        let mut stores = shardpack_storage::StoreRegistry::new();
        stores.insert("default", store.clone()).unwrap();
        let mut cached = DatasetReader::resume_with_registry_and_cache(
            stores.clone(),
            layout.clone(),
            &checkpoint,
            cache.clone(),
        )
        .await
        .unwrap();
        let mut remaining = Vec::new();
        while let Some(record) = cached.next_record().await.unwrap() {
            remaining.push(record.sample_id[0]);
        }
        assert_eq!(remaining, vec![2, 3, 4, 5]);
        assert!(cache.path_for(shard_id).is_file());
        let metrics = cache.metrics().snapshot();
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.downloaded_bytes, summary.byte_size);

        let mut corrupt = tokio::fs::read(cache.path_for(shard_id)).await.unwrap();
        corrupt[shardpack_format::SHARD_HEADER_SIZE + shardpack_format::BLOCK_HEADER_SIZE] ^= 0x80;
        tokio::fs::write(cache.path_for(shard_id), corrupt)
            .await
            .unwrap();
        let mut cached = DatasetReader::resume_with_registry_and_cache(
            stores.clone(),
            layout.clone(),
            &later_checkpoint,
            cache.clone(),
        )
        .await
        .unwrap();
        let mut remaining = Vec::new();
        while let Some(record) = cached.next_record().await.unwrap() {
            remaining.push(record.sample_id[0]);
        }
        assert_eq!(remaining, vec![4, 5]);
        assert_eq!(cache.metrics().snapshot().corrupt_entries, 1);

        store.delete(object_uri).await.unwrap();
        let mut cached = DatasetReader::resume_with_registry_and_cache(
            stores,
            layout,
            &checkpoint,
            cache.clone(),
        )
        .await
        .unwrap();
        let mut remaining = Vec::new();
        while let Some(record) = cached.next_record().await.unwrap() {
            remaining.push(record.sample_id[0]);
        }
        assert_eq!(remaining, vec![2, 3, 4, 5]);
        assert_eq!(cache.metrics().snapshot().hits, 2);
    }

    #[tokio::test]
    async fn retries_corrupt_transport_from_a_block_boundary() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([6; 16], 0, b"payload").unwrap();
        let (valid, summary) = writer.finish().unwrap();
        let mut corrupt = valid.clone();
        *corrupt.last_mut().unwrap() ^= 0x80;
        let object_uri = "shards/transient.spk";
        store
            .write_immutable(
                object_uri,
                Box::pin(stream::once(async move { Ok(Bytes::from(corrupt)) })),
            )
            .await
            .unwrap();
        let mut layout = proto::LayoutManifest {
            snapshot_id: vec![6; 32],
            builder_version: "retry-test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1024,
                target_block_bytes: 1024,
                block_codec: "none".into(),
                shuffle_seed: None,
                max_shard_bytes: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            total_records: 1,
            total_bytes: summary.byte_size,
            content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            shards: vec![proto::ShardMeta {
                shard_id: summary.shard_id.to_vec(),
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: object_uri.into(),
                }),
                byte_size: summary.byte_size,
                record_count: 1,
                block_count: 1,
                content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            }],
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();

        let replacement_path = root.path().join(object_uri);
        let replacement = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            tokio::fs::write(replacement_path, valid).await.unwrap();
        });
        let metrics = RuntimeMetrics::default();
        let mut reader = DatasetReader::new(store, layout)
            .unwrap()
            .with_metrics(metrics.clone())
            .with_retry_policy(RetryPolicy {
                max_retries: 2,
                initial_backoff: std::time::Duration::from_millis(30),
                max_backoff: std::time::Duration::from_millis(30),
            });
        let record = reader.next_record().await.unwrap().unwrap();
        assert_eq!(record.payload, "payload");
        assert!(reader.next_record().await.unwrap().is_none());
        replacement.await.unwrap();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.retries, 1);
        assert_eq!(snapshot.checksum_failures, 1);
    }

    #[tokio::test]
    async fn invalidates_a_corrupt_cache_hit_and_refetches_once() {
        use tokio::io::AsyncReadExt;

        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([7; 16], 0, b"payload").unwrap();
        let (valid, summary) = writer.finish().unwrap();
        let object_uri = "shards/cached.spk";
        store
            .write_immutable(
                object_uri,
                Box::pin(stream::once({
                    let valid = valid.clone();
                    async move { Ok(Bytes::from(valid)) }
                })),
            )
            .await
            .unwrap();
        let id = shardpack_manifest::ShardId(summary.shard_id);
        let cache = shardpack_cache::LocalCache::new(cache_root.path());
        let mut populate = cache
            .open(&store, object_uri, id, summary.byte_size)
            .await
            .unwrap();
        let mut ignored = Vec::new();
        populate.read_to_end(&mut ignored).await.unwrap();
        let mut corrupt = valid.clone();
        *corrupt.last_mut().unwrap() ^= 0x80;
        tokio::fs::write(cache.path_for(id), corrupt).await.unwrap();

        let mut layout = proto::LayoutManifest {
            snapshot_id: vec![7; 32],
            builder_version: "cache-retry-test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1024,
                target_block_bytes: 1024,
                block_codec: "none".into(),
                shuffle_seed: None,
                max_shard_bytes: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            total_records: 1,
            total_bytes: summary.byte_size,
            content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            shards: vec![proto::ShardMeta {
                shard_id: summary.shard_id.to_vec(),
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: object_uri.into(),
                }),
                byte_size: summary.byte_size,
                record_count: 1,
                block_count: 1,
                content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
            }],
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();
        let mut reader = DatasetReader::new(store, layout)
            .unwrap()
            .with_cache(cache.clone())
            .with_retry_policy(RetryPolicy {
                max_retries: 1,
                initial_backoff: std::time::Duration::ZERO,
                max_backoff: std::time::Duration::ZERO,
            });
        let record = reader.next_record().await.unwrap().unwrap();
        assert_eq!(record.payload, "payload");
        assert!(reader.next_record().await.unwrap().is_none());
        assert_eq!(tokio::fs::read(cache.path_for(id)).await.unwrap(), valid);
        let metrics = cache.metrics().snapshot();
        assert_eq!(metrics.corrupt_entries, 1);
        assert_eq!(metrics.misses, 2);
    }
}
