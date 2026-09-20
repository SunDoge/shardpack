use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::{
    DatasetReader, DecryptionContext, EncodedRecord, Error, LoaderCheckpoint, NetworkPrefetch,
    Result, RuntimeMetrics, ShuffleLimits, ShufflePool, ShufflePoolCheckpoint,
};

pub trait ByteSize {
    fn byte_size(&self) -> usize;
}

impl ByteSize for EncodedRecord {
    fn byte_size(&self) -> usize {
        self.payload.len()
    }
}

impl ByteSize for bytes::Bytes {
    fn byte_size(&self) -> usize {
        self.len()
    }
}

pub trait RecordDecoder: Send + Sync + 'static {
    type Output: ByteSize + Send + 'static;
    type Error: std::fmt::Display + Send + 'static;

    fn decode(&self, record: EncodedRecord) -> std::result::Result<Self::Output, Self::Error>;
}

pub struct RegistryRecordDecoder<T> {
    registry: Arc<shardpack_codec::CodecRegistry<T>>,
    descriptor: shardpack_manifest::proto::RecordSchemaDescriptor,
}

impl<T: 'static> RegistryRecordDecoder<T> {
    pub fn new(
        registry: Arc<shardpack_codec::CodecRegistry<T>>,
        descriptor: shardpack_manifest::proto::RecordSchemaDescriptor,
    ) -> shardpack_codec::Result<Self> {
        registry.validate_descriptor(&descriptor)?;
        Ok(Self {
            registry,
            descriptor,
        })
    }
}

impl<T> RecordDecoder for RegistryRecordDecoder<T>
where
    T: ByteSize + Send + 'static,
    shardpack_codec::CodecRegistry<T>: Send + Sync,
{
    type Output = T;
    type Error = shardpack_codec::Error;

    fn decode(&self, record: EncodedRecord) -> std::result::Result<T, Self::Error> {
        self.registry.decode(&self.descriptor, record.payload)
    }
}

pub trait RecordTransform<I>: Send + Sync + 'static {
    type Output: ByteSize + Send + 'static;
    type Error: std::fmt::Display + Send + 'static;

    fn transform(&self, input: I) -> std::result::Result<Self::Output, Self::Error>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeLimits {
    pub network_concurrency: usize,
    pub network_prefetch_bytes: usize,
    pub cache_max_bytes: u64,
    pub shuffle_max_records: usize,
    pub shuffle_max_bytes: usize,
    pub decode_workers: usize,
    pub decode_queue_bytes: usize,
    pub transform_workers: usize,
    pub transform_queue_bytes: usize,
    pub batch_prefetch: usize,
    pub batch_queue_bytes: usize,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            network_concurrency: 2,
            network_prefetch_bytes: 64 * 1024 * 1024,
            cache_max_bytes: 256 * 1024 * 1024 * 1024,
            shuffle_max_records: 4096,
            shuffle_max_bytes: 256 * 1024 * 1024,
            decode_workers: 4,
            decode_queue_bytes: 256 * 1024 * 1024,
            transform_workers: 4,
            transform_queue_bytes: 256 * 1024 * 1024,
            batch_prefetch: 2,
            batch_queue_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PipelineConfig {
    pub limits: RuntimeLimits,
    pub batch_records: usize,
    pub shuffle_seed: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            limits: RuntimeLimits::default(),
            batch_records: 32,
            shuffle_seed: 1,
        }
    }
}

pub struct TrainingPipeline<T> {
    output: mpsc::Receiver<Result<BudgetedBatch<T>>>,
    cancelled: Arc<AtomicBool>,
    checkpoint_requested: Arc<AtomicBool>,
    checkpoint: Option<oneshot::Receiver<PipelineCheckpoint>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelineCheckpoint {
    pub loader: LoaderCheckpoint,
    pub shuffle: ShufflePoolCheckpoint,
    pub next_sequence: u64,
    pub source_exhausted: bool,
}

pub struct QuiescedPipeline<T> {
    /// Batches which were already admitted before the checkpoint boundary.
    /// Process these batches before durably committing `checkpoint`.
    pub pending_batches: Vec<Vec<T>>,
    pub checkpoint: PipelineCheckpoint,
}

struct Sequenced<T> {
    sequence: u64,
    value: T,
}

impl<T: ByteSize> ByteSize for Sequenced<T> {
    fn byte_size(&self) -> usize {
        self.value.byte_size()
    }
}

impl<T: Send + 'static> TrainingPipeline<T> {
    pub fn start<D, X>(
        reader: DatasetReader,
        decoder: D,
        transform: X,
        config: PipelineConfig,
    ) -> Result<Self>
    where
        D: RecordDecoder,
        X: RecordTransform<D::Output, Output = T>,
        T: ByteSize,
    {
        Self::start_inner(reader, decoder, transform, config, None)
    }

    pub async fn resume<D, X>(
        store: shardpack_storage::Store,
        layout: shardpack_manifest::proto::LayoutManifest,
        checkpoint: PipelineCheckpoint,
        decoder: D,
        transform: X,
        config: PipelineConfig,
    ) -> Result<Self>
    where
        D: RecordDecoder,
        X: RecordTransform<D::Output, Output = T>,
        T: ByteSize,
    {
        let reader = DatasetReader::resume(store, layout, &checkpoint.loader).await?;
        Self::start_inner(reader, decoder, transform, config, Some(checkpoint))
    }

    pub async fn resume_encrypted<D, X>(
        store: shardpack_storage::Store,
        layout: shardpack_manifest::proto::LayoutManifest,
        checkpoint: PipelineCheckpoint,
        decryption: DecryptionContext,
        decoder: D,
        transform: X,
        config: PipelineConfig,
    ) -> Result<Self>
    where
        D: RecordDecoder,
        X: RecordTransform<D::Output, Output = T>,
        T: ByteSize,
    {
        let reader = DatasetReader::resume_encrypted(
            store,
            layout,
            &checkpoint.loader,
            &decryption.key_id,
            decryption.key,
        )
        .await?;
        Self::start_inner(reader, decoder, transform, config, Some(checkpoint))
    }

    fn start_inner<D, X>(
        reader: DatasetReader,
        decoder: D,
        transform: X,
        config: PipelineConfig,
        restored: Option<PipelineCheckpoint>,
    ) -> Result<Self>
    where
        D: RecordDecoder,
        X: RecordTransform<D::Output, Output = T>,
        T: ByteSize,
    {
        validate_config(&config)?;
        let [
            decode_queue_records,
            transform_queue_records,
            batch_queue_records,
        ] = queue_record_capacities(&config)?;
        let reader = reader
            .with_cache_max_bytes(config.limits.cache_max_bytes)
            .with_network_prefetch(NetworkPrefetch {
                concurrency: config.limits.network_concurrency,
                max_bytes: config.limits.network_prefetch_bytes,
            })?;
        let metrics = reader.metrics();
        let cancelled = Arc::new(AtomicBool::new(false));
        let checkpoint_requested = Arc::new(AtomicBool::new(false));
        let (checkpoint_tx, checkpoint_rx) = oneshot::channel();
        let (encoded_tx, encoded_rx) = byte_channel(
            decode_queue_records,
            config.limits.decode_queue_bytes,
            metrics.clone(),
        );
        let (decoded_tx, decoded_rx) = byte_channel(
            transform_queue_records,
            config.limits.transform_queue_bytes,
            metrics.clone(),
        );
        let (transformed_tx, transformed_rx) = byte_channel(
            batch_queue_records,
            config.limits.transform_queue_bytes,
            metrics.clone(),
        );
        let (output_tx, output) = mpsc::channel(config.limits.batch_prefetch);
        let batch_budget = Arc::new(Semaphore::new(config.limits.batch_queue_bytes));
        let decoder = Arc::new(decoder);
        let transform = Arc::new(transform);
        let encoded_rx = Arc::new(Mutex::new(encoded_rx));
        let decoded_rx = Arc::new(Mutex::new(decoded_rx));
        let mut tasks = Vec::new();

        let batch_start_sequence = restored.as_ref().map_or(0, |value| value.next_sequence);
        tasks.push(tokio::spawn(fetch_and_shuffle(
            reader,
            encoded_tx,
            output_tx.clone(),
            FetchControl {
                cancelled: cancelled.clone(),
                checkpoint_requested: checkpoint_requested.clone(),
                checkpoint_sender: checkpoint_tx,
                config,
                restored,
            },
        )));
        for _ in 0..config.limits.decode_workers {
            tasks.push(tokio::spawn(decode_worker(
                encoded_rx.clone(),
                decoded_tx.clone(),
                decoder.clone(),
                output_tx.clone(),
                cancelled.clone(),
            )));
        }
        drop(decoded_tx);
        for _ in 0..config.limits.transform_workers {
            tasks.push(tokio::spawn(transform_worker(
                decoded_rx.clone(),
                transformed_tx.clone(),
                transform.clone(),
                output_tx.clone(),
                cancelled.clone(),
            )));
        }
        drop(transformed_tx);
        tasks.push(tokio::spawn(batch_worker(
            transformed_rx,
            output_tx.clone(),
            BatchControl {
                cancelled: cancelled.clone(),
                batch_records: config.batch_records,
                next_sequence: batch_start_sequence,
                budget: batch_budget,
                max_bytes: config.limits.batch_queue_bytes,
                metrics,
            },
        )));
        drop(output_tx);
        Ok(Self {
            output,
            cancelled,
            checkpoint_requested,
            checkpoint: Some(checkpoint_rx),
            tasks,
        })
    }

    pub async fn next_batch(&mut self) -> Option<Result<Vec<T>>> {
        let next = self
            .output
            .recv()
            .await
            .map(|result| result.map(|batch| batch.values));
        if next.as_ref().is_some_and(Result::is_err) {
            self.cancelled.store(true, Ordering::Release);
            for task in &self.tasks {
                task.abort();
            }
        }
        next
    }

    pub async fn shutdown(mut self) {
        self.cancelled.store(true, Ordering::Release);
        for task in self.tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
    }

    /// Stops admission at a deterministic batch boundary, drains all admitted
    /// work, and returns a checkpoint for the first record after those batches.
    /// The returned batches must be processed before committing the checkpoint.
    pub async fn quiesce(mut self) -> Result<QuiescedPipeline<T>> {
        self.checkpoint_requested.store(true, Ordering::Release);
        let mut pending_batches = Vec::new();
        while let Some(batch) = self.output.recv().await {
            pending_batches.push(batch?.values);
        }
        let checkpoint = self
            .checkpoint
            .take()
            .expect("checkpoint receiver exists")
            .await
            .map_err(|_| Error::CheckpointUnavailable)?;
        for task in self.tasks.drain(..) {
            task.await.map_err(|source| Error::PipelineStage {
                stage: "quiesce",
                message: source.to_string(),
            })?;
        }
        Ok(QuiescedPipeline {
            pending_batches,
            checkpoint,
        })
    }
}

impl<T> Drop for TrainingPipeline<T> {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        for task in &self.tasks {
            task.abort();
        }
    }
}

struct Budgeted<T> {
    value: T,
    _permit: OwnedSemaphorePermit,
}

struct BudgetedBatch<T> {
    values: Vec<T>,
    _permits: Vec<OwnedSemaphorePermit>,
}

struct ByteSender<T> {
    sender: mpsc::Sender<Budgeted<T>>,
    budget: Arc<Semaphore>,
    max_bytes: usize,
    metrics: RuntimeMetrics,
}

impl<T> Clone for ByteSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            budget: self.budget.clone(),
            max_bytes: self.max_bytes,
            metrics: self.metrics.clone(),
        }
    }
}

struct ByteReceiver<T>(mpsc::Receiver<Budgeted<T>>);

struct FetchControl {
    cancelled: Arc<AtomicBool>,
    checkpoint_requested: Arc<AtomicBool>,
    checkpoint_sender: oneshot::Sender<PipelineCheckpoint>,
    config: PipelineConfig,
    restored: Option<PipelineCheckpoint>,
}

struct BatchControl {
    cancelled: Arc<AtomicBool>,
    batch_records: usize,
    next_sequence: u64,
    budget: Arc<Semaphore>,
    max_bytes: usize,
    metrics: RuntimeMetrics,
}

fn byte_channel<T>(
    records: usize,
    bytes: usize,
    metrics: RuntimeMetrics,
) -> (ByteSender<T>, ByteReceiver<T>) {
    let (sender, receiver) = mpsc::channel(records);
    (
        ByteSender {
            sender,
            budget: Arc::new(Semaphore::new(bytes)),
            max_bytes: bytes,
            metrics,
        },
        ByteReceiver(receiver),
    )
}

impl<T> ByteSender<T> {
    async fn send(&self, value: T, bytes: usize, stage: &'static str) -> Result<()> {
        if bytes > self.max_bytes || bytes > u32::MAX as usize {
            return Err(Error::RecordTooLarge {
                bytes,
                limit: self.max_bytes.min(u32::MAX as usize),
            });
        }
        let permits = u32::try_from(bytes.max(1)).expect("bounded by u32::MAX");
        let permit = match self.budget.clone().try_acquire_many_owned(permits) {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                self.metrics.queue_backpressure();
                self.budget
                    .clone()
                    .acquire_many_owned(permits)
                    .await
                    .map_err(|_| Error::QueueClosed { stage })?
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(Error::QueueClosed { stage });
            }
        };
        self.sender
            .send(Budgeted {
                value,
                _permit: permit,
            })
            .await
            .map_err(|_| Error::QueueClosed { stage })
    }
}

impl<T> ByteReceiver<T> {
    async fn recv(&mut self) -> Option<Budgeted<T>> {
        self.0.recv().await
    }
}

async fn fetch_and_shuffle<T>(
    mut reader: DatasetReader,
    encoded_tx: ByteSender<Sequenced<EncodedRecord>>,
    errors: mpsc::Sender<Result<BudgetedBatch<T>>>,
    control: FetchControl,
) where
    T: Send + 'static,
{
    let FetchControl {
        cancelled,
        checkpoint_requested,
        checkpoint_sender,
        config,
        restored,
    } = control;
    let limits = ShuffleLimits {
        max_records: config.limits.shuffle_max_records,
        max_bytes: config.limits.shuffle_max_bytes,
    };
    let (mut pool, mut next_sequence, mut source_exhausted) = match restored {
        Some(checkpoint) => {
            if checkpoint.shuffle.limits != limits {
                fail_pipeline(
                    &errors,
                    &cancelled,
                    Error::InvalidCheckpoint {
                        reason: "shuffle limits differ from pipeline configuration",
                    },
                )
                .await;
                return;
            }
            let pool = match ShufflePool::restore(checkpoint.shuffle) {
                Ok(pool) => pool,
                Err(error) => {
                    fail_pipeline(&errors, &cancelled, error).await;
                    return;
                }
            };
            (pool, checkpoint.next_sequence, checkpoint.source_exhausted)
        }
        None => (ShufflePool::new(limits, config.shuffle_seed), 0, false),
    };
    let result: Result<PipelineCheckpoint> = async {
        while !cancelled.load(Ordering::Acquire) {
            if checkpoint_requested.load(Ordering::Acquire)
                && next_sequence % config.batch_records as u64 == 0
            {
                break;
            }
            if source_exhausted {
                while let Some(emitted) = pool.drain_next() {
                    let item = Sequenced {
                        sequence: next_sequence,
                        value: emitted,
                    };
                    next_sequence += 1;
                    let size = item.byte_size();
                    encoded_tx.send(item, size, "fetch").await?;
                }
                break;
            }
            let Some(record) = reader.next_record().await? else {
                source_exhausted = true;
                continue;
            };
            if let Some(emitted) = pool.push(record)? {
                let item = Sequenced {
                    sequence: next_sequence,
                    value: emitted,
                };
                next_sequence += 1;
                let size = item.byte_size();
                encoded_tx.send(item, size, "fetch").await?;
            }
        }
        Ok(PipelineCheckpoint {
            loader: reader.checkpoint(),
            shuffle: pool.checkpoint(),
            next_sequence,
            source_exhausted,
        })
    }
    .await;
    match result {
        Ok(checkpoint) => {
            let _ = checkpoint_sender.send(checkpoint);
        }
        Err(error) => fail_pipeline(&errors, &cancelled, error).await,
    }
}

async fn decode_worker<D, T>(
    input: Arc<Mutex<ByteReceiver<Sequenced<EncodedRecord>>>>,
    output: ByteSender<Sequenced<D::Output>>,
    decoder: Arc<D>,
    errors: mpsc::Sender<Result<BudgetedBatch<T>>>,
    cancelled: Arc<AtomicBool>,
) where
    D: RecordDecoder,
    T: Send + 'static,
{
    while !cancelled.load(Ordering::Acquire) {
        let Some(input) = input.lock().await.recv().await else {
            return;
        };
        let decoder = decoder.clone();
        let sequence = input.value.sequence;
        let decoded = tokio::task::spawn_blocking(move || decoder.decode(input.value.value)).await;
        let decoded = match decoded {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                fail_pipeline(
                    &errors,
                    &cancelled,
                    Error::PipelineStage {
                        stage: "decode",
                        message: error.to_string(),
                    },
                )
                .await;
                return;
            }
            Err(error) => {
                fail_pipeline(
                    &errors,
                    &cancelled,
                    Error::PipelineStage {
                        stage: "decode",
                        message: error.to_string(),
                    },
                )
                .await;
                return;
            }
        };
        let decoded = Sequenced {
            sequence,
            value: decoded,
        };
        let size = decoded.byte_size();
        if let Err(error) = output.send(decoded, size, "decode").await {
            fail_pipeline(&errors, &cancelled, error).await;
            return;
        }
    }
}

async fn transform_worker<D, X, T>(
    input: Arc<Mutex<ByteReceiver<Sequenced<D>>>>,
    output: ByteSender<Sequenced<T>>,
    transform: Arc<X>,
    errors: mpsc::Sender<Result<BudgetedBatch<T>>>,
    cancelled: Arc<AtomicBool>,
) where
    D: Send + 'static,
    X: RecordTransform<D, Output = T>,
    T: ByteSize + Send + 'static,
{
    while !cancelled.load(Ordering::Acquire) {
        let Some(input) = input.lock().await.recv().await else {
            return;
        };
        let transform = transform.clone();
        let sequence = input.value.sequence;
        let transformed =
            tokio::task::spawn_blocking(move || transform.transform(input.value.value)).await;
        let transformed = match transformed {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                fail_pipeline(
                    &errors,
                    &cancelled,
                    Error::PipelineStage {
                        stage: "transform",
                        message: error.to_string(),
                    },
                )
                .await;
                return;
            }
            Err(error) => {
                fail_pipeline(
                    &errors,
                    &cancelled,
                    Error::PipelineStage {
                        stage: "transform",
                        message: error.to_string(),
                    },
                )
                .await;
                return;
            }
        };
        let transformed = Sequenced {
            sequence,
            value: transformed,
        };
        let size = transformed.byte_size();
        if let Err(error) = output.send(transformed, size, "transform").await {
            fail_pipeline(&errors, &cancelled, error).await;
            return;
        }
    }
}

async fn batch_worker<T: ByteSize + Send + 'static>(
    mut input: ByteReceiver<Sequenced<T>>,
    output: mpsc::Sender<Result<BudgetedBatch<T>>>,
    control: BatchControl,
) {
    let BatchControl {
        cancelled,
        batch_records,
        mut next_sequence,
        budget: batch_budget,
        max_bytes: batch_max_bytes,
        metrics,
    } = control;
    let mut batch = Vec::with_capacity(batch_records);
    let mut batch_permits = Vec::with_capacity(batch_records);
    let mut batch_bytes = 0_usize;
    let mut reorder = BTreeMap::new();
    while !cancelled.load(Ordering::Acquire) {
        match input.recv().await {
            Some(item) => {
                reorder.insert(item.value.sequence, (item.value.value, item._permit));
                while let Some((value, input_permit)) = reorder.remove(&next_sequence) {
                    next_sequence += 1;
                    let value_bytes = value.byte_size().max(1);
                    let Some(next_batch_bytes) = batch_bytes.checked_add(value_bytes) else {
                        cancelled.store(true, Ordering::Release);
                        let _ = output
                            .send(Err(Error::RecordTooLarge {
                                bytes: usize::MAX,
                                limit: batch_max_bytes,
                            }))
                            .await;
                        return;
                    };
                    if next_batch_bytes > batch_max_bytes || value_bytes > u32::MAX as usize {
                        cancelled.store(true, Ordering::Release);
                        let _ = output
                            .send(Err(Error::RecordTooLarge {
                                bytes: next_batch_bytes,
                                limit: batch_max_bytes.min(u32::MAX as usize),
                            }))
                            .await;
                        return;
                    }
                    let permits = u32::try_from(value_bytes).expect("bounded by u32::MAX");
                    let batch_permit = match batch_budget.clone().try_acquire_many_owned(permits) {
                        Ok(permit) => permit,
                        Err(tokio::sync::TryAcquireError::NoPermits) => {
                            metrics.queue_backpressure();
                            match batch_budget.clone().acquire_many_owned(permits).await {
                                Ok(permit) => permit,
                                Err(_) => return,
                            }
                        }
                        Err(tokio::sync::TryAcquireError::Closed) => return,
                    };
                    drop(input_permit);
                    batch.push(value);
                    batch_permits.push(batch_permit);
                    batch_bytes = next_batch_bytes;
                    if batch.len() == batch_records {
                        let completed = BudgetedBatch {
                            values: std::mem::replace(
                                &mut batch,
                                Vec::with_capacity(batch_records),
                            ),
                            _permits: std::mem::replace(
                                &mut batch_permits,
                                Vec::with_capacity(batch_records),
                            ),
                        };
                        batch_bytes = 0;
                        if output.send(Ok(completed)).await.is_err() {
                            return;
                        }
                    }
                }
            }
            None => {
                if !reorder.is_empty() {
                    let _ = output
                        .send(Err(Error::PipelineStage {
                            stage: "batch",
                            message: "sequence gap after worker shutdown".into(),
                        }))
                        .await;
                    return;
                }
                if !batch.is_empty() {
                    let _ = output
                        .send(Ok(BudgetedBatch {
                            values: batch,
                            _permits: batch_permits,
                        }))
                        .await;
                }
                return;
            }
        }
    }
}

async fn fail_pipeline<T>(
    output: &mpsc::Sender<Result<BudgetedBatch<T>>>,
    cancelled: &AtomicBool,
    error: Error,
) {
    if !cancelled.swap(true, Ordering::AcqRel) {
        let _ = output.send(Err(error)).await;
    }
}

fn validate_config(config: &PipelineConfig) -> Result<()> {
    if config.limits.cache_max_bytes == 0 {
        return Err(Error::InvalidLimit {
            field: "cache_max_bytes",
            reason: "must be greater than zero",
        });
    }
    for (field, value) in [
        ("network_concurrency", config.limits.network_concurrency),
        (
            "network_prefetch_bytes",
            config.limits.network_prefetch_bytes,
        ),
        ("shuffle_max_records", config.limits.shuffle_max_records),
        ("shuffle_max_bytes", config.limits.shuffle_max_bytes),
        ("decode_workers", config.limits.decode_workers),
        ("decode_queue_bytes", config.limits.decode_queue_bytes),
        ("transform_workers", config.limits.transform_workers),
        ("transform_queue_bytes", config.limits.transform_queue_bytes),
        ("batch_prefetch", config.limits.batch_prefetch),
        ("batch_queue_bytes", config.limits.batch_queue_bytes),
        ("batch_records", config.batch_records),
    ] {
        if value == 0 {
            return Err(Error::InvalidLimit {
                field,
                reason: "must be greater than zero",
            });
        }
    }
    for (field, value) in [
        ("decode_queue_bytes", config.limits.decode_queue_bytes),
        ("transform_queue_bytes", config.limits.transform_queue_bytes),
        ("batch_queue_bytes", config.limits.batch_queue_bytes),
    ] {
        if value > u32::MAX as usize {
            return Err(Error::InvalidLimit {
                field,
                reason: "must fit the semaphore permit range",
            });
        }
    }
    Ok(())
}

fn queue_record_capacities(config: &PipelineConfig) -> Result<[usize; 3]> {
    let decode = config
        .limits
        .decode_workers
        .checked_mul(2)
        .ok_or(Error::InvalidLimit {
            field: "decode_workers",
            reason: "queue capacity overflows usize",
        })?;
    let transform = config
        .limits
        .transform_workers
        .checked_mul(2)
        .ok_or(Error::InvalidLimit {
            field: "transform_workers",
            reason: "queue capacity overflows usize",
        })?;
    let batches = config
        .batch_records
        .checked_mul(config.limits.batch_prefetch)
        .ok_or(Error::InvalidLimit {
            field: "batch_records",
            reason: "batch prefetch capacity overflows usize",
        })?;
    Ok([decode, transform, batches])
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use bytes::Bytes;
    use futures_util::stream;
    use shardpack_format::{ContentFingerprint, ShardWriter, WriterOptions};
    use shardpack_manifest::{layout_id, proto};

    use super::{
        BatchControl, PipelineConfig, RecordDecoder, RecordTransform, RuntimeLimits, Sequenced,
        TrainingPipeline, batch_worker, byte_channel, queue_record_capacities,
    };
    use crate::{DatasetReader, EncodedRecord, RuntimeMetrics};

    struct Decode;
    impl RecordDecoder for Decode {
        type Output = Bytes;
        type Error = std::convert::Infallible;

        fn decode(&self, record: EncodedRecord) -> std::result::Result<Self::Output, Self::Error> {
            Ok(record.payload)
        }
    }

    struct SlowDecode;
    impl RecordDecoder for SlowDecode {
        type Output = Bytes;
        type Error = std::convert::Infallible;

        fn decode(&self, record: EncodedRecord) -> std::result::Result<Self::Output, Self::Error> {
            std::thread::sleep(std::time::Duration::from_millis(1));
            Ok(record.payload)
        }
    }

    struct Transform;
    impl RecordTransform<Bytes> for Transform {
        type Output = Bytes;
        type Error = std::convert::Infallible;

        fn transform(&self, mut input: Bytes) -> std::result::Result<Self::Output, Self::Error> {
            let value = input[0].wrapping_add(1);
            input = Bytes::from(vec![value; input.len()]);
            Ok(input)
        }
    }

    fn fingerprint(value: ContentFingerprint) -> proto::ContentFingerprint {
        proto::ContentFingerprint {
            record_count: value.record_count,
            accumulator_a: value.accumulator_a.to_vec(),
            accumulator_b: value.accumulator_b.to_vec(),
        }
    }

    #[test]
    fn queue_record_capacity_overflow_is_rejected() {
        let mut config = PipelineConfig::default();
        config.limits.decode_workers = usize::MAX;
        assert!(matches!(
            queue_record_capacities(&config),
            Err(crate::Error::InvalidLimit {
                field: "decode_workers",
                ..
            })
        ));

        config.limits.decode_workers = 1;
        config.batch_records = usize::MAX;
        assert!(matches!(
            queue_record_capacities(&config),
            Err(crate::Error::InvalidLimit {
                field: "batch_records",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn transformed_byte_budget_follows_queued_batches() {
        let (sender, receiver) = byte_channel(4, 8, RuntimeMetrics::default());
        let transform_budget = Arc::clone(&sender.budget);
        let batch_budget = Arc::new(tokio::sync::Semaphore::new(8));
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker = tokio::spawn(batch_worker(
            receiver,
            output_tx,
            BatchControl {
                cancelled: Arc::clone(&cancelled),
                batch_records: 2,
                next_sequence: 0,
                budget: Arc::clone(&batch_budget),
                max_bytes: 8,
                metrics: RuntimeMetrics::default(),
            },
        ));

        sender
            .send(
                Sequenced {
                    sequence: 0,
                    value: Bytes::from_static(b"abcd"),
                },
                4,
                "test",
            )
            .await
            .unwrap();
        sender
            .send(
                Sequenced {
                    sequence: 1,
                    value: Bytes::from_static(b"efgh"),
                },
                4,
                "test",
            )
            .await
            .unwrap();
        drop(sender);

        while output_rx.is_empty() {
            tokio::task::yield_now().await;
        }
        assert_eq!(transform_budget.available_permits(), 8);
        assert_eq!(batch_budget.available_permits(), 0);
        let batch = output_rx.recv().await.unwrap().unwrap();
        assert_eq!(batch.values.len(), 2);
        drop(batch);
        assert_eq!(batch_budget.available_permits(), 8);
        cancelled.store(true, Ordering::Release);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn batch_larger_than_byte_budget_fails_instead_of_deadlocking() {
        let (sender, receiver) = byte_channel(4, 8, RuntimeMetrics::default());
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker = tokio::spawn(batch_worker(
            receiver,
            output_tx,
            BatchControl {
                cancelled: Arc::clone(&cancelled),
                batch_records: 2,
                next_sequence: 0,
                budget: Arc::new(tokio::sync::Semaphore::new(7)),
                max_bytes: 7,
                metrics: RuntimeMetrics::default(),
            },
        ));
        for (sequence, payload) in [b"abcd", b"efgh"].into_iter().enumerate() {
            sender
                .send(
                    Sequenced {
                        sequence: sequence as u64,
                        value: Bytes::copy_from_slice(payload),
                    },
                    payload.len(),
                    "test",
                )
                .await
                .unwrap();
        }
        drop(sender);

        assert!(matches!(
            output_rx.recv().await.unwrap(),
            Err(crate::Error::RecordTooLarge { bytes: 8, limit: 7 })
        ));
        assert!(cancelled.load(Ordering::Acquire));
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_pipeline_decodes_transforms_and_batches_all_records() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        for value in 0..7_u8 {
            writer.add_record([value; 16], 0, &[value; 3]).unwrap();
        }
        let (bytes, summary) = writer.finish().unwrap();
        store
            .write_immutable(
                "shards/pipeline.spk",
                Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) })),
            )
            .await
            .unwrap();
        let mut layout = proto::LayoutManifest {
            snapshot_id: vec![4; 32],
            builder_version: "pipeline-test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 1024,
                target_block_bytes: 1024,
                block_codec: "none".into(),
                shuffle_seed: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            total_records: 7,
            total_bytes: summary.byte_size,
            content_fingerprint: Some(fingerprint(summary.content_fingerprint)),
            shards: vec![proto::ShardMeta {
                shard_id: summary.shard_id.to_vec(),
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: "shards/pipeline.spk".into(),
                }),
                byte_size: summary.byte_size,
                record_count: 7,
                block_count: summary.block_count,
                content_fingerprint: Some(fingerprint(summary.content_fingerprint)),
            }],
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();
        let reader = DatasetReader::new(store, layout).unwrap();
        let mut pipeline = TrainingPipeline::start(
            reader,
            Decode,
            Transform,
            PipelineConfig {
                limits: RuntimeLimits {
                    shuffle_max_records: 2,
                    shuffle_max_bytes: 8,
                    decode_workers: 2,
                    decode_queue_bytes: 8,
                    transform_workers: 2,
                    transform_queue_bytes: 8,
                    batch_prefetch: 1,
                    ..RuntimeLimits::default()
                },
                batch_records: 3,
                shuffle_seed: 9,
            },
        )
        .unwrap();
        let mut values = Vec::new();
        let mut batch_sizes = Vec::new();
        while let Some(batch) = pipeline.next_batch().await {
            let batch = batch.unwrap();
            batch_sizes.push(batch.len());
            values.extend(batch.into_iter().map(|value| value[0]));
        }
        values.sort_unstable();
        batch_sizes.sort_unstable();
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(batch_sizes, vec![1, 3, 3]);
    }

    #[tokio::test]
    async fn quiesce_and_resume_matches_uninterrupted_record_order() {
        let root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let mut writer = ShardWriter::new(
            Vec::new(),
            WriterOptions {
                target_block_bytes: 17,
                ..WriterOptions::default()
            },
        )
        .unwrap();
        for value in 0..40_u8 {
            writer.add_record([value; 16], 0, &[value; 3]).unwrap();
        }
        let (bytes, summary) = writer.finish().unwrap();
        store
            .write_immutable(
                "shards/checkpoint.spk",
                Box::pin(stream::once(async move { Ok(Bytes::from(bytes)) })),
            )
            .await
            .unwrap();
        let mut layout = proto::LayoutManifest {
            snapshot_id: vec![5; 32],
            builder_version: "checkpoint-test".into(),
            recipe: Some(proto::LayoutRecipe {
                target_shard_bytes: 4096,
                target_block_bytes: 17,
                block_codec: "none".into(),
                shuffle_seed: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            total_records: 40,
            total_bytes: summary.byte_size,
            content_fingerprint: Some(fingerprint(summary.content_fingerprint)),
            shards: vec![proto::ShardMeta {
                shard_id: summary.shard_id.to_vec(),
                location: Some(proto::ObjectLocation {
                    store_id: "default".into(),
                    object_key: "shards/checkpoint.spk".into(),
                }),
                byte_size: summary.byte_size,
                record_count: 40,
                block_count: summary.block_count,
                content_fingerprint: Some(fingerprint(summary.content_fingerprint)),
            }],
            state: proto::LayoutState::Complete as i32,
            ..Default::default()
        };
        layout.layout_id = layout_id(&layout).0.to_vec();
        let config = PipelineConfig {
            limits: RuntimeLimits {
                shuffle_max_records: 5,
                shuffle_max_bytes: 18,
                decode_workers: 3,
                decode_queue_bytes: 12,
                transform_workers: 3,
                transform_queue_bytes: 12,
                batch_prefetch: 1,
                ..RuntimeLimits::default()
            },
            batch_records: 4,
            shuffle_seed: 91,
        };

        let mut uninterrupted = TrainingPipeline::start(
            DatasetReader::new(store.clone(), layout.clone()).unwrap(),
            SlowDecode,
            Transform,
            config,
        )
        .unwrap();
        let mut expected = Vec::new();
        while let Some(batch) = uninterrupted.next_batch().await {
            expected.extend(batch.unwrap().into_iter().map(|value| value[0]));
        }

        let mut first = TrainingPipeline::start(
            DatasetReader::new(store.clone(), layout.clone()).unwrap(),
            SlowDecode,
            Transform,
            config,
        )
        .unwrap();
        let mut actual = first
            .next_batch()
            .await
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|value| value[0])
            .collect::<Vec<_>>();
        let quiesced = first.quiesce().await.unwrap();
        assert_eq!(quiesced.checkpoint.next_sequence % 4, 0);
        for batch in quiesced.pending_batches {
            actual.extend(batch.into_iter().map(|value| value[0]));
        }
        let mut resumed = TrainingPipeline::resume(
            store,
            layout,
            quiesced.checkpoint,
            SlowDecode,
            Transform,
            config,
        )
        .await
        .unwrap();
        while let Some(batch) = resumed.next_batch().await {
            actual.extend(batch.unwrap().into_iter().map(|value| value[0]));
        }
        assert_eq!(actual, expected);
    }
}
