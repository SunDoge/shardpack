use std::{
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    thread,
};

use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use bytes::Bytes;
use prost::Message;
use pyo3::{
    buffer::PyBuffer,
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::{PyAny, PyBytes, PyDict},
};
use shardpack_cache::{CacheMetrics, LocalCache};
use shardpack_core::{
    EncodedRecord as CoreEncodedRecord, MaterializeEncryption, MaterializeOptions,
    Result as CoreResult, SourceReader, materialize_layout,
};
use shardpack_format::BlockCodec;
use shardpack_manifest::proto;
use shardpack_runtime::{
    DatasetReader, DistributedContext, EncodedRecord, LoaderCheckpoint, NetworkPrefetch,
    NodeAffinity, RuntimeMetrics, ShuffleLimits, ShufflePool, ShufflePoolCheckpoint,
};
use shardpack_storage::StoreRegistry;

const DEFAULT_WRITER_MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_WRITER_MAX_BATCH_RECORDS: usize = 1_000_000;

struct BatchSource {
    receiver: mpsc::Receiver<Vec<CoreEncodedRecord>>,
    current: std::vec::IntoIter<CoreEncodedRecord>,
}

impl SourceReader for BatchSource {
    fn next_record(&mut self) -> CoreResult<Option<CoreEncodedRecord>> {
        loop {
            if let Some(record) = self.current.next() {
                return Ok(Some(record));
            }
            match self.receiver.recv() {
                Ok(batch) => self.current = batch.into_iter(),
                Err(_) => return Ok(None),
            }
        }
    }
}

#[derive(Clone)]
enum WriterOutcome {
    Success(String),
    Failure(String),
}

struct WriterState {
    sender: Option<mpsc::SyncSender<Vec<CoreEncodedRecord>>>,
    worker: Option<thread::JoinHandle<WriterOutcome>>,
    outcome: Option<WriterOutcome>,
}

#[pyclass(module = "shardpack._native", name = "DatasetWriter")]
struct PyDatasetWriter {
    process_id: u32,
    max_batch_bytes: usize,
    max_batch_records: usize,
    admission: Mutex<()>,
    state: Mutex<WriterState>,
}

impl PyDatasetWriter {
    fn check_process(&self) -> PyResult<()> {
        if std::process::id() != self.process_id {
            return Err(PyRuntimeError::new_err(
                "DatasetWriter cannot be used after fork; create a new writer in the child",
            ));
        }
        Ok(())
    }

    fn copy_u8_buffer(
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        name: &str,
    ) -> PyResult<(Vec<u8>, usize, Vec<usize>)> {
        let buffer = PyBuffer::<u8>::get(value)
            .map_err(|error| PyValueError::new_err(format!("{name}: {error}")))?;
        if !buffer.is_c_contiguous() {
            return Err(PyValueError::new_err(format!(
                "{name} must be C-contiguous"
            )));
        }
        let dimensions = buffer.dimensions();
        let shape = buffer.shape().to_vec();
        Ok((buffer.to_vec(py)?, dimensions, shape))
    }

    fn copy_u64_buffer(py: Python<'_>, value: &Bound<'_, PyAny>, name: &str) -> PyResult<Vec<u64>> {
        let buffer = PyBuffer::<u64>::get(value)
            .map_err(|error| PyValueError::new_err(format!("{name}: {error}")))?;
        if buffer.dimensions() != 1 || !buffer.is_c_contiguous() {
            return Err(PyValueError::new_err(format!(
                "{name} must be a one-dimensional C-contiguous uint64 buffer"
            )));
        }
        buffer.to_vec(py)
    }

    fn copy_u32_buffer(py: Python<'_>, value: &Bound<'_, PyAny>, name: &str) -> PyResult<Vec<u32>> {
        let buffer = PyBuffer::<u32>::get(value)
            .map_err(|error| PyValueError::new_err(format!("{name}: {error}")))?;
        if buffer.dimensions() != 1 || !buffer.is_c_contiguous() {
            return Err(PyValueError::new_err(format!(
                "{name} must be a one-dimensional C-contiguous uint32 buffer"
            )));
        }
        buffer.to_vec(py)
    }
}

#[pymethods]
impl PyDatasetWriter {
    #[new]
    #[pyo3(signature = (config, snapshot_id, staging_dir, *, store=None, target_shard_bytes=1_073_741_824, max_shard_bytes=None, target_block_bytes=16_777_216, block_codec="none", zstd_level=1, object_prefix="shards", layout_prefix="layouts", queue_batches=2, max_batch_records=DEFAULT_WRITER_MAX_BATCH_RECORDS, max_batch_bytes=DEFAULT_WRITER_MAX_BATCH_BYTES, encryption_key_id=None, encryption_key=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        config: PathBuf,
        snapshot_id: &Bound<'_, PyBytes>,
        staging_dir: PathBuf,
        store: Option<String>,
        target_shard_bytes: u64,
        max_shard_bytes: Option<u64>,
        target_block_bytes: usize,
        block_codec: &str,
        zstd_level: i32,
        object_prefix: &str,
        layout_prefix: &str,
        queue_batches: usize,
        max_batch_records: usize,
        max_batch_bytes: usize,
        encryption_key_id: Option<String>,
        encryption_key: Option<&Bound<'_, PyBytes>>,
    ) -> PyResult<Self> {
        if queue_batches == 0 {
            return Err(PyValueError::new_err(
                "queue_batches must be greater than zero",
            ));
        }
        if max_batch_bytes == 0 {
            return Err(PyValueError::new_err(
                "max_batch_bytes must be greater than zero",
            ));
        }
        if max_batch_records == 0 {
            return Err(PyValueError::new_err(
                "max_batch_records must be greater than zero",
            ));
        }
        let snapshot_id: [u8; 32] = snapshot_id
            .as_bytes()
            .try_into()
            .map_err(|_| PyValueError::new_err("snapshot_id must contain exactly 32 bytes"))?;
        let block_codec = match block_codec {
            "none" => BlockCodec::None,
            "zstd" => BlockCodec::Zstd,
            _ => {
                return Err(PyValueError::new_err(
                    "block_codec must be 'none' or 'zstd'",
                ));
            }
        };
        let encryption = parse_encryption(encryption_key_id, encryption_key)?
            .map(|(key_id, key)| MaterializeEncryption { key_id, key });
        std::fs::create_dir_all(&staging_dir).map_err(runtime_error)?;
        let config = shardpack_config::ShardPackConfig::load(&config).map_err(runtime_error)?;
        let store_id = config
            .selected_store_id(store.as_deref())
            .map_err(runtime_error)?
            .to_owned();
        let stores = config.build_registry().map_err(runtime_error)?;
        let store = stores.get(&store_id).map_err(runtime_error)?.clone();
        let options = MaterializeOptions {
            store_id,
            target_shard_bytes,
            max_shard_bytes,
            target_block_bytes,
            block_codec,
            zstd_level,
            object_prefix: object_prefix.to_owned(),
            layout_prefix: layout_prefix.to_owned(),
            encryption,
            ..Default::default()
        };
        let (sender, receiver) = mpsc::sync_channel(queue_batches);
        let worker = thread::Builder::new()
            .name("shardpack-python-writer".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => return WriterOutcome::Failure(error.to_string()),
                };
                let mut source = BatchSource {
                    receiver,
                    current: Vec::new().into_iter(),
                };
                match runtime.block_on(materialize_layout(
                    &mut source,
                    snapshot_id,
                    &store,
                    &staging_dir,
                    &options,
                )) {
                    Ok(materialization) => {
                        WriterOutcome::Success(materialization.layout_object_key)
                    }
                    Err(error) => WriterOutcome::Failure(error.to_string()),
                }
            })
            .map_err(runtime_error)?;
        Ok(Self {
            process_id: std::process::id(),
            max_batch_bytes,
            max_batch_records,
            admission: Mutex::new(()),
            state: Mutex::new(WriterState {
                sender: Some(sender),
                worker: Some(worker),
                outcome: None,
            }),
        })
    }

    #[pyo3(signature = (payload_data, offsets, sample_ids, flags=None))]
    fn write_batch(
        &self,
        py: Python<'_>,
        payload_data: &Bound<'_, PyAny>,
        offsets: &Bound<'_, PyAny>,
        sample_ids: &Bound<'_, PyAny>,
        flags: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<usize> {
        self.check_process()?;
        let (payload_data, payload_dimensions, _) =
            Self::copy_u8_buffer(py, payload_data, "payload_data")?;
        if payload_dimensions != 1 {
            return Err(PyValueError::new_err(
                "payload_data must be a one-dimensional uint8 buffer",
            ));
        }
        if payload_data.len() > self.max_batch_bytes {
            return Err(PyValueError::new_err(format!(
                "payload_data exceeds max_batch_bytes ({})",
                self.max_batch_bytes
            )));
        }
        let offsets = Self::copy_u64_buffer(py, offsets, "offsets")?;
        if offsets.is_empty() || offsets[0] != 0 {
            return Err(PyValueError::new_err(
                "offsets must contain at least one element and start at zero",
            ));
        }
        let record_count = offsets.len() - 1;
        if record_count > self.max_batch_records {
            return Err(PyValueError::new_err(format!(
                "batch exceeds max_batch_records ({})",
                self.max_batch_records
            )));
        }
        let (sample_ids, sample_id_dimensions, sample_id_shape) =
            Self::copy_u8_buffer(py, sample_ids, "sample_ids")?;
        let valid_sample_id_shape = match sample_id_dimensions {
            1 => sample_ids.len() == record_count.saturating_mul(16),
            2 => sample_id_shape.as_slice() == [record_count, 16],
            _ => false,
        };
        if !valid_sample_id_shape {
            return Err(PyValueError::new_err(
                "sample_ids must have shape (record_count, 16) or be a flat buffer of that size",
            ));
        }
        let flags = match flags {
            Some(flags) => Self::copy_u32_buffer(py, flags, "flags")?,
            None => vec![0; record_count],
        };
        if flags.len() != record_count {
            return Err(PyValueError::new_err(
                "flags length must equal record_count",
            ));
        }
        let payload_data = Bytes::from(payload_data);
        let mut records = Vec::with_capacity(record_count);
        for index in 0..record_count {
            let start = usize::try_from(offsets[index])
                .map_err(|_| PyValueError::new_err("offset overflows usize"))?;
            let end = usize::try_from(offsets[index + 1])
                .map_err(|_| PyValueError::new_err("offset overflows usize"))?;
            if start > end || end > payload_data.len() {
                return Err(PyValueError::new_err(
                    "offsets must be monotonic and within payload_data",
                ));
            }
            let id_start = index * 16;
            let sample_id = sample_ids[id_start..id_start + 16]
                .try_into()
                .expect("validated sample ID buffer length");
            records.push(CoreEncodedRecord {
                sample_id,
                flags: flags[index],
                payload: payload_data.slice(start..end),
            });
        }
        if offsets.last().copied() != Some(payload_data.len() as u64) {
            return Err(PyValueError::new_err(
                "the final offset must equal payload_data length",
            ));
        }
        if records.is_empty() {
            return Ok(0);
        }
        py.detach(|| {
            let _admission = self.admission.lock().map_err(|_| {
                PyRuntimeError::new_err("DatasetWriter admission lock was poisoned")
            })?;
            let state = self.state.lock().map_err(|_| {
                PyRuntimeError::new_err("DatasetWriter state was poisoned by another thread")
            })?;
            let sender = state
                .sender
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("DatasetWriter is already finished"))?
                .clone();
            drop(state);
            sender
                .send(records)
                .map_err(|_| PyRuntimeError::new_err("DatasetWriter worker stopped unexpectedly"))
        })?;
        Ok(record_count)
    }

    fn finish(&self, py: Python<'_>) -> PyResult<String> {
        self.check_process()?;
        let outcome = py.detach(|| -> PyResult<WriterOutcome> {
            let _admission = self.admission.lock().map_err(|_| {
                PyRuntimeError::new_err("DatasetWriter admission lock was poisoned")
            })?;
            let mut state = self.state.lock().map_err(|_| {
                PyRuntimeError::new_err("DatasetWriter state was poisoned by another thread")
            })?;
            if let Some(outcome) = &state.outcome {
                return Ok(outcome.clone());
            }
            state.sender.take();
            let worker = state.worker.take().ok_or_else(|| {
                PyRuntimeError::new_err("DatasetWriter finish is already in progress")
            })?;
            drop(state);
            let outcome = worker
                .join()
                .unwrap_or_else(|_| WriterOutcome::Failure("DatasetWriter worker panicked".into()));
            let mut state = self.state.lock().map_err(|_| {
                PyRuntimeError::new_err("DatasetWriter state was poisoned by another thread")
            })?;
            state.outcome = Some(outcome.clone());
            Ok(outcome)
        })?;
        match outcome {
            WriterOutcome::Success(key) => Ok(key),
            WriterOutcome::Failure(error) => Err(PyRuntimeError::new_err(error)),
        }
    }
}

impl Drop for PyDatasetWriter {
    fn drop(&mut self) {
        if let Ok(state) = self.state.get_mut() {
            state.sender.take();
            if let Some(worker) = state.worker.take() {
                let _ = worker.join();
            }
        }
    }
}

fn manifest_runtime() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

fn loader_runtime(network_concurrency: usize) -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(network_concurrency.clamp(1, 4))
        .thread_name("shardpack-python")
        .build()
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

const PYTHON_CHECKPOINT_VERSION: u32 = 1;
const CHECKPOINT_ENVELOPE_MAGIC: &[u8; 8] = b"SPKCPY01";
const CHECKPOINT_PLAIN: u8 = 0;
const CHECKPOINT_AES_256_GCM: u8 = 1;
const CHECKPOINT_NONCE_BYTES: usize = 12;
const CHECKPOINT_TAG_BYTES: usize = 16;

#[derive(Clone, PartialEq, Message)]
struct CheckpointWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "2")]
    snapshot_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    layout_id: Vec<u8>,
    #[prost(uint64, tag = "4")]
    epoch: u64,
    #[prost(uint64, tag = "5")]
    shard_seed: u64,
    #[prost(uint64, tag = "6")]
    next_shard_index: u64,
    #[prost(bytes = "vec", optional, tag = "7")]
    current_shard_id: Option<Vec<u8>>,
    #[prost(uint64, tag = "8")]
    records_yielded_in_current_shard: u64,
    #[prost(uint32, tag = "9")]
    block_index: u32,
    #[prost(uint64, tag = "10")]
    block_start_offset: u64,
    #[prost(uint32, tag = "11")]
    record_index: u32,
    #[prost(message, optional, tag = "12")]
    distributed: Option<DistributedWire>,
    #[prost(message, optional, tag = "13")]
    node_affinity: Option<NodeAffinityWire>,
    #[prost(message, optional, tag = "14")]
    shuffle: Option<ShuffleWire>,
    #[prost(bool, tag = "15")]
    source_exhausted: bool,
    #[prost(bool, tag = "16")]
    requires_encryption: bool,
}

#[derive(Clone, PartialEq, Message)]
struct DistributedWire {
    #[prost(uint64, tag = "1")]
    rank: u64,
    #[prost(uint64, tag = "2")]
    world_size: u64,
    #[prost(uint64, tag = "3")]
    worker: u64,
    #[prost(uint64, tag = "4")]
    workers_per_rank: u64,
}

#[derive(Clone, PartialEq, Message)]
struct NodeAffinityWire {
    #[prost(string, tag = "1")]
    node_id: String,
    #[prost(string, repeated, tag = "2")]
    node_ids: Vec<String>,
    #[prost(uint64, tag = "3")]
    local_rank: u64,
    #[prost(uint64, tag = "4")]
    local_world_size: u64,
    #[prost(uint64, tag = "5")]
    worker: u64,
    #[prost(uint64, tag = "6")]
    workers_per_rank: u64,
    #[prost(uint64, tag = "7")]
    affinity_seed: u64,
}

#[derive(Clone, PartialEq, Message)]
struct ShuffleWire {
    #[prost(uint64, tag = "1")]
    max_records: u64,
    #[prost(uint64, tag = "2")]
    max_bytes: u64,
    #[prost(message, repeated, tag = "3")]
    records: Vec<RecordWire>,
    #[prost(uint64, tag = "4")]
    buffered_bytes: u64,
    #[prost(uint64, tag = "5")]
    rng_state: u64,
}

#[derive(Clone, PartialEq, Message)]
struct RecordWire {
    #[prost(bytes = "vec", tag = "1")]
    sample_id: Vec<u8>,
    #[prost(uint32, tag = "2")]
    flags: u32,
    #[prost(bytes = "bytes", tag = "3")]
    payload: Bytes,
}

fn checkpoint_value_error(message: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(format!("invalid loader checkpoint: {message}"))
}

fn usize_from_u64(value: u64, field: &str) -> PyResult<usize> {
    usize::try_from(value).map_err(|_| checkpoint_value_error(format!("{field} overflows usize")))
}

fn array_from_bytes<const N: usize>(value: Vec<u8>, field: &str) -> PyResult<[u8; N]> {
    value
        .try_into()
        .map_err(|_| checkpoint_value_error(format!("{field} must contain {N} bytes")))
}

fn encode_checkpoint(
    loader: LoaderCheckpoint,
    shuffle: Option<ShufflePoolCheckpoint>,
    source_exhausted: bool,
    requires_encryption: bool,
) -> Bytes {
    let distributed = loader.distributed.map(|value| DistributedWire {
        rank: value.rank as u64,
        world_size: value.world_size as u64,
        worker: value.worker as u64,
        workers_per_rank: value.workers_per_rank as u64,
    });
    let node_affinity = loader.node_affinity.map(|value| NodeAffinityWire {
        node_id: value.node_id,
        node_ids: value.node_ids,
        local_rank: value.local_rank as u64,
        local_world_size: value.local_world_size as u64,
        worker: value.worker as u64,
        workers_per_rank: value.workers_per_rank as u64,
        affinity_seed: value.affinity_seed,
    });
    let shuffle = shuffle.map(|value| ShuffleWire {
        max_records: value.limits.max_records as u64,
        max_bytes: value.limits.max_bytes as u64,
        records: value
            .records
            .into_iter()
            .map(|record| RecordWire {
                sample_id: record.sample_id.to_vec(),
                flags: record.flags,
                payload: record.payload,
            })
            .collect(),
        buffered_bytes: value.buffered_bytes as u64,
        rng_state: value.rng_state,
    });
    CheckpointWire {
        version: PYTHON_CHECKPOINT_VERSION,
        snapshot_id: loader.snapshot_id.to_vec(),
        layout_id: loader.layout_id.to_vec(),
        epoch: loader.epoch,
        shard_seed: loader.shard_seed,
        next_shard_index: loader.next_shard_index as u64,
        current_shard_id: loader.current_shard_id.map(|value| value.to_vec()),
        records_yielded_in_current_shard: loader.records_yielded_in_current_shard,
        block_index: loader.block_index,
        block_start_offset: loader.block_start_offset,
        record_index: loader.record_index,
        distributed,
        node_affinity,
        shuffle,
        source_exhausted,
        requires_encryption,
    }
    .encode_to_vec()
    .into()
}

fn decode_checkpoint(
    encoded: Bytes,
) -> PyResult<(LoaderCheckpoint, Option<ShufflePoolCheckpoint>, bool, bool)> {
    let wire = CheckpointWire::decode(encoded).map_err(checkpoint_value_error)?;
    if wire.version != PYTHON_CHECKPOINT_VERSION {
        return Err(checkpoint_value_error(format!(
            "unsupported version {}",
            wire.version
        )));
    }
    let distributed = wire
        .distributed
        .map(|value| -> PyResult<DistributedContext> {
            Ok(DistributedContext {
                rank: usize_from_u64(value.rank, "distributed.rank")?,
                world_size: usize_from_u64(value.world_size, "distributed.world_size")?,
                worker: usize_from_u64(value.worker, "distributed.worker")?,
                workers_per_rank: usize_from_u64(
                    value.workers_per_rank,
                    "distributed.workers_per_rank",
                )?,
            })
        })
        .transpose()?;
    let node_affinity = wire
        .node_affinity
        .map(|value| -> PyResult<NodeAffinity> {
            Ok(NodeAffinity {
                node_id: value.node_id,
                node_ids: value.node_ids,
                local_rank: usize_from_u64(value.local_rank, "node_affinity.local_rank")?,
                local_world_size: usize_from_u64(
                    value.local_world_size,
                    "node_affinity.local_world_size",
                )?,
                worker: usize_from_u64(value.worker, "node_affinity.worker")?,
                workers_per_rank: usize_from_u64(
                    value.workers_per_rank,
                    "node_affinity.workers_per_rank",
                )?,
                affinity_seed: value.affinity_seed,
            })
        })
        .transpose()?;
    let shuffle = wire
        .shuffle
        .map(|value| -> PyResult<ShufflePoolCheckpoint> {
            let checkpoint = ShufflePoolCheckpoint {
                limits: ShuffleLimits {
                    max_records: usize_from_u64(value.max_records, "shuffle.max_records")?,
                    max_bytes: usize_from_u64(value.max_bytes, "shuffle.max_bytes")?,
                },
                records: value
                    .records
                    .into_iter()
                    .map(|record| {
                        Ok(EncodedRecord {
                            sample_id: array_from_bytes(record.sample_id, "record.sample_id")?,
                            flags: record.flags,
                            payload: record.payload,
                        })
                    })
                    .collect::<PyResult<Vec<_>>>()?,
                buffered_bytes: usize_from_u64(value.buffered_bytes, "shuffle.buffered_bytes")?,
                rng_state: value.rng_state,
            };
            ShufflePool::validate_checkpoint(&checkpoint).map_err(checkpoint_value_error)?;
            Ok(checkpoint)
        })
        .transpose()?;
    Ok((
        LoaderCheckpoint {
            snapshot_id: array_from_bytes(wire.snapshot_id, "snapshot_id")?,
            layout_id: array_from_bytes(wire.layout_id, "layout_id")?,
            epoch: wire.epoch,
            shard_seed: wire.shard_seed,
            next_shard_index: usize_from_u64(wire.next_shard_index, "next_shard_index")?,
            current_shard_id: wire
                .current_shard_id
                .map(|value| array_from_bytes(value, "current_shard_id"))
                .transpose()?,
            records_yielded_in_current_shard: wire.records_yielded_in_current_shard,
            block_index: wire.block_index,
            block_start_offset: wire.block_start_offset,
            record_index: wire.record_index,
            distributed,
            node_affinity,
        },
        shuffle,
        wire.source_exhausted,
        wire.requires_encryption,
    ))
}

fn checkpoint_cipher(key: &[u8; 32]) -> LessSafeKey {
    let derived = blake3::derive_key("shardpack python loader checkpoint v1", key);
    LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &derived).expect("AES-256 key has fixed length"))
}

fn checkpoint_key(value: Option<&Bound<'_, PyBytes>>) -> PyResult<Option<[u8; 32]>> {
    value
        .map(|value| {
            value.as_bytes().try_into().map_err(|_| {
                PyValueError::new_err("checkpoint encryption key must contain exactly 32 bytes")
            })
        })
        .transpose()
}

fn seal_checkpoint(plaintext: &[u8], key: &[u8; 32]) -> PyResult<Vec<u8>> {
    let mut nonce = [0_u8; CHECKPOINT_NONCE_BYTES];
    getrandom::fill(&mut nonce).map_err(runtime_error)?;
    let mut body = plaintext.to_vec();
    let tag = checkpoint_cipher(key)
        .seal_in_place_separate_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(CHECKPOINT_ENVELOPE_MAGIC.as_slice()),
            &mut body,
        )
        .map_err(|_| PyValueError::new_err("failed to encrypt loader checkpoint"))?;
    let mut output = Vec::with_capacity(
        CHECKPOINT_ENVELOPE_MAGIC.len()
            + 1
            + CHECKPOINT_NONCE_BYTES
            + body.len()
            + CHECKPOINT_TAG_BYTES,
    );
    output.extend_from_slice(CHECKPOINT_ENVELOPE_MAGIC);
    output.push(CHECKPOINT_AES_256_GCM);
    output.extend_from_slice(&nonce);
    output.extend_from_slice(&body);
    output.extend_from_slice(tag.as_ref());
    Ok(output)
}

fn open_checkpoint(value: &[u8], key: Option<&[u8; 32]>) -> PyResult<(Bytes, bool)> {
    let Some((&mode, body)) = value
        .strip_prefix(CHECKPOINT_ENVELOPE_MAGIC)
        .and_then(|value| value.split_first())
    else {
        return Err(checkpoint_value_error("invalid envelope magic"));
    };
    match mode {
        CHECKPOINT_PLAIN => Ok((Bytes::copy_from_slice(body), false)),
        CHECKPOINT_AES_256_GCM => {
            let key = key.ok_or_else(|| {
                PyValueError::new_err("encrypted loader checkpoint requires encryption_key")
            })?;
            if body.len() < CHECKPOINT_NONCE_BYTES + CHECKPOINT_TAG_BYTES {
                return Err(checkpoint_value_error("truncated encrypted envelope"));
            }
            let (nonce, ciphertext_and_tag) = body.split_at(CHECKPOINT_NONCE_BYTES);
            let (ciphertext, tag) =
                ciphertext_and_tag.split_at(ciphertext_and_tag.len() - CHECKPOINT_TAG_BYTES);
            let mut plaintext = ciphertext.to_vec();
            checkpoint_cipher(key)
                .open_in_place_separate_tag(
                    Nonce::assume_unique_for_key(
                        nonce.try_into().expect("checked checkpoint nonce length"),
                    ),
                    Aad::from(CHECKPOINT_ENVELOPE_MAGIC.as_slice()),
                    tag,
                    &mut plaintext,
                )
                .map_err(|_| {
                    PyValueError::new_err(
                        "loader checkpoint authentication failed; key or bytes are incorrect",
                    )
                })?;
            Ok((plaintext.into(), true))
        }
        _ => Err(checkpoint_value_error("unsupported envelope mode")),
    }
}

#[pyclass(frozen, module = "shardpack._native", name = "LoaderCheckpoint")]
struct PyLoaderCheckpoint {
    encoded: Bytes,
    requires_encryption: bool,
}

#[pymethods]
impl PyLoaderCheckpoint {
    #[staticmethod]
    #[pyo3(signature = (value, encryption_key=None))]
    fn from_bytes(
        value: &Bound<'_, PyBytes>,
        encryption_key: Option<&Bound<'_, PyBytes>>,
    ) -> PyResult<Self> {
        let key = checkpoint_key(encryption_key)?;
        let (encoded, encrypted_envelope) = open_checkpoint(value.as_bytes(), key.as_ref())?;
        let (_, _, _, requires_encryption) = decode_checkpoint(encoded.clone())?;
        if requires_encryption && !encrypted_envelope {
            return Err(PyValueError::new_err(
                "checkpoint for an encrypted layout must use an encrypted envelope",
            ));
        }
        Ok(Self {
            encoded,
            requires_encryption,
        })
    }

    #[pyo3(signature = (encryption_key=None))]
    fn to_bytes<'py>(
        &self,
        py: Python<'py>,
        encryption_key: Option<&Bound<'_, PyBytes>>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let key = checkpoint_key(encryption_key)?;
        if self.requires_encryption && key.is_none() {
            return Err(PyValueError::new_err(
                "checkpoint for an encrypted layout requires encryption_key",
            ));
        }
        let envelope = if let Some(key) = key {
            seal_checkpoint(&self.encoded, &key)?
        } else {
            let mut envelope =
                Vec::with_capacity(CHECKPOINT_ENVELOPE_MAGIC.len() + 1 + self.encoded.len());
            envelope.extend_from_slice(CHECKPOINT_ENVELOPE_MAGIC);
            envelope.push(CHECKPOINT_PLAIN);
            envelope.extend_from_slice(&self.encoded);
            envelope
        };
        Ok(PyBytes::new(py, &envelope))
    }

    fn __len__(&self) -> usize {
        self.encoded.len()
    }
}

#[pyclass(frozen, module = "shardpack._native", name = "Record")]
struct PyRecord {
    sample_id: [u8; 16],
    flags: u32,
    payload: Py<PyBytes>,
    payload_len: usize,
}

impl PyRecord {
    fn from_encoded(py: Python<'_>, record: EncodedRecord) -> Self {
        let payload_len = record.payload.len();
        Self {
            sample_id: record.sample_id,
            flags: record.flags,
            payload: PyBytes::new(py, &record.payload).unbind(),
            payload_len,
        }
    }
}

#[pymethods]
impl PyRecord {
    #[getter]
    fn sample_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.sample_id)
    }

    #[getter]
    const fn flags(&self) -> u32 {
        self.flags
    }

    #[getter]
    fn payload(&self, py: Python<'_>) -> Py<PyBytes> {
        self.payload.clone_ref(py)
    }

    fn __len__(&self) -> usize {
        self.payload_len
    }

    fn __repr__(&self) -> String {
        format!(
            "Record(sample_id='{}', flags={}, payload_bytes={})",
            hex::encode(self.sample_id),
            self.flags,
            self.payload_len
        )
    }
}

#[derive(Clone)]
struct LoaderOptions {
    cache_dir: Option<PathBuf>,
    cache_max_bytes: u64,
    network_concurrency: usize,
    network_prefetch_bytes: usize,
    epoch: u64,
    shard_seed: u64,
    rank: usize,
    world_size: usize,
    worker: usize,
    workers_per_rank: usize,
    encryption: Option<(String, shardpack_format::EncryptionKey)>,
    shuffle_max_records: usize,
    shuffle_max_bytes: usize,
    shuffle_seed: u64,
}

impl LoaderOptions {
    fn configure_reader(
        &self,
        mut reader: DatasetReader,
        metrics: RuntimeMetrics,
        cache_metrics: Option<CacheMetrics>,
        apply_encryption: bool,
    ) -> PyResult<DatasetReader> {
        reader = reader.with_metrics(metrics);
        if let Some(cache_dir) = &self.cache_dir {
            let cache = LocalCache::new(cache_dir).with_max_bytes(self.cache_max_bytes);
            reader = reader.with_cache(if let Some(metrics) = cache_metrics {
                cache.with_metrics(metrics)
            } else {
                cache
            });
        }
        if apply_encryption && let Some((key_id, key)) = &self.encryption {
            reader = reader
                .with_decryption_key(key_id, key.clone())
                .map_err(runtime_error)?;
        }
        reader
            .with_network_prefetch(NetworkPrefetch {
                concurrency: self.network_concurrency,
                max_bytes: self.network_prefetch_bytes,
            })
            .map_err(runtime_error)
    }

    fn reader(
        &self,
        stores: StoreRegistry,
        layout: Arc<proto::LayoutManifest>,
        metrics: RuntimeMetrics,
    ) -> PyResult<(DatasetReader, Option<CacheMetrics>)> {
        let context = DistributedContext {
            rank: self.rank,
            world_size: self.world_size,
            worker: self.worker,
            workers_per_rank: self.workers_per_rank,
        };
        let reader = DatasetReader::new_assigned_with_registry(
            stores,
            layout,
            self.epoch,
            self.shard_seed,
            context,
        )
        .map_err(runtime_error)?;
        let cache_metrics = self.cache_dir.as_ref().map(|_| CacheMetrics::default());
        let reader = self.configure_reader(reader, metrics, cache_metrics.clone(), true)?;
        Ok((reader, cache_metrics))
    }
}

#[pyclass(frozen, module = "shardpack._native", name = "Dataset")]
struct PyDataset {
    stores: StoreRegistry,
    layout: Arc<proto::LayoutManifest>,
    layout_key: String,
}

#[pymethods]
impl PyDataset {
    #[new]
    #[pyo3(signature = (config, layout, store=None))]
    fn new(
        py: Python<'_>,
        config: PathBuf,
        layout: String,
        store: Option<String>,
    ) -> PyResult<Self> {
        let config = shardpack_config::ShardPackConfig::load(&config).map_err(runtime_error)?;
        let store_id = config
            .selected_store_id(store.as_deref())
            .map_err(runtime_error)?
            .to_owned();
        let stores = config.build_registry().map_err(runtime_error)?;
        let manifest_store = stores.get(&store_id).map_err(runtime_error)?.clone();
        let runtime = manifest_runtime()?;
        let manifest = py
            .detach(|| runtime.block_on(shardpack_core::load_layout(&manifest_store, &layout)))
            .map_err(runtime_error)?;
        Ok(Self {
            stores,
            layout: Arc::new(manifest),
            layout_key: layout,
        })
    }

    #[getter]
    fn layout_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.layout.layout_id)
    }

    #[getter]
    fn record_count(&self) -> u64 {
        self.layout.total_records
    }

    #[getter]
    fn shard_count(&self) -> usize {
        self.layout.shards.len()
    }

    #[getter]
    fn stored_bytes(&self) -> u64 {
        self.layout.total_bytes
    }

    #[getter]
    fn layout_key(&self) -> &str {
        &self.layout_key
    }

    #[pyo3(signature = (*, cache_dir=None, cache_max_bytes=274_877_906_944, network_concurrency=2, network_prefetch_bytes=67_108_864, shuffle_max_records=4096, shuffle_max_bytes=268_435_456, shuffle_seed=1, epoch=0, shard_seed=0, rank=0, world_size=1, worker=0, workers_per_rank=1, encryption_key_id=None, encryption_key=None))]
    #[allow(clippy::too_many_arguments)]
    fn records(
        &self,
        cache_dir: Option<PathBuf>,
        cache_max_bytes: u64,
        network_concurrency: usize,
        network_prefetch_bytes: usize,
        shuffle_max_records: usize,
        shuffle_max_bytes: usize,
        shuffle_seed: u64,
        epoch: u64,
        shard_seed: u64,
        rank: usize,
        world_size: usize,
        worker: usize,
        workers_per_rank: usize,
        encryption_key_id: Option<String>,
        encryption_key: Option<&Bound<'_, PyBytes>>,
    ) -> PyResult<PyRecordLoader> {
        PyRecordLoader::build(
            self.stores.clone(),
            self.layout.clone(),
            LoaderOptions {
                cache_dir,
                cache_max_bytes,
                network_concurrency,
                network_prefetch_bytes,
                epoch,
                shard_seed,
                rank,
                world_size,
                worker,
                workers_per_rank,
                encryption: parse_encryption(encryption_key_id, encryption_key)?,
                shuffle_max_records,
                shuffle_max_bytes,
                shuffle_seed,
            },
        )
    }

    #[pyo3(signature = (checkpoint, *, cache_dir=None, cache_max_bytes=274_877_906_944, network_concurrency=2, network_prefetch_bytes=67_108_864, encryption_key_id=None, encryption_key=None))]
    #[allow(clippy::too_many_arguments)]
    fn resume_records(
        &self,
        py: Python<'_>,
        checkpoint: &PyLoaderCheckpoint,
        cache_dir: Option<PathBuf>,
        cache_max_bytes: u64,
        network_concurrency: usize,
        network_prefetch_bytes: usize,
        encryption_key_id: Option<String>,
        encryption_key: Option<&Bound<'_, PyBytes>>,
    ) -> PyResult<PyRecordLoader> {
        let (
            loader_checkpoint,
            shuffle_checkpoint,
            source_exhausted,
            checkpoint_requires_encryption,
        ) = decode_checkpoint(checkpoint.encoded.clone())?;
        if checkpoint_requires_encryption != self.layout.encryption.is_some() {
            return Err(checkpoint_value_error(
                "encryption requirement differs from the selected layout",
            ));
        }
        let encryption = parse_encryption(encryption_key_id, encryption_key)?;
        let runtime = loader_runtime(network_concurrency)?;
        let cache_metrics = cache_dir.as_ref().map(|_| CacheMetrics::default());
        let resume_cache = cache_dir.as_ref().map(|directory| {
            let cache = LocalCache::new(directory).with_max_bytes(cache_max_bytes);
            if let Some(metrics) = &cache_metrics {
                cache.with_metrics(metrics.clone())
            } else {
                cache
            }
        });
        let reader = if let Some((key_id, key)) = &encryption {
            let stores = self.stores.clone();
            let layout = self.layout.clone();
            let key_id = key_id.clone();
            let key = key.clone();
            let resume_cache = resume_cache.clone();
            py.detach(|| {
                runtime.block_on(async {
                    if let Some(cache) = resume_cache {
                        DatasetReader::resume_encrypted_with_registry_and_cache(
                            stores,
                            layout,
                            &loader_checkpoint,
                            &key_id,
                            key,
                            cache,
                        )
                        .await
                    } else {
                        DatasetReader::resume_encrypted_with_registry(
                            stores,
                            layout,
                            &loader_checkpoint,
                            &key_id,
                            key,
                        )
                        .await
                    }
                })
            })
        } else {
            let stores = self.stores.clone();
            let layout = self.layout.clone();
            py.detach(|| {
                runtime.block_on(async {
                    if let Some(cache) = resume_cache {
                        DatasetReader::resume_with_registry_and_cache(
                            stores,
                            layout,
                            &loader_checkpoint,
                            cache,
                        )
                        .await
                    } else {
                        DatasetReader::resume_with_registry(stores, layout, &loader_checkpoint)
                            .await
                    }
                })
            })
        }
        .map_err(runtime_error)?;
        let metrics = RuntimeMetrics::default();
        let options = LoaderOptions {
            cache_dir,
            cache_max_bytes,
            network_concurrency,
            network_prefetch_bytes,
            epoch: loader_checkpoint.epoch,
            shard_seed: loader_checkpoint.shard_seed,
            rank: 0,
            world_size: 1,
            worker: 0,
            workers_per_rank: 1,
            encryption,
            shuffle_max_records: 0,
            shuffle_max_bytes: 0,
            shuffle_seed: 1,
        };
        let reader =
            options.configure_reader(reader, metrics.clone(), cache_metrics.clone(), false)?;
        let shuffle = shuffle_checkpoint
            .map(ShufflePool::restore)
            .transpose()
            .map_err(runtime_error)?;
        Ok(PyRecordLoader {
            runtime,
            reader,
            metrics,
            cache_metrics,
            shuffle,
            source_exhausted,
            checkpoint_requires_encryption,
        })
    }

    #[pyo3(signature = (batch_size, *, drop_last=false, cache_dir=None, cache_max_bytes=274_877_906_944, network_concurrency=2, network_prefetch_bytes=67_108_864, shuffle_max_records=4096, shuffle_max_bytes=268_435_456, shuffle_seed=1, epoch=0, shard_seed=0, rank=0, world_size=1, worker=0, workers_per_rank=1, encryption_key_id=None, encryption_key=None))]
    #[allow(clippy::too_many_arguments)]
    fn batches(
        &self,
        batch_size: usize,
        drop_last: bool,
        cache_dir: Option<PathBuf>,
        cache_max_bytes: u64,
        network_concurrency: usize,
        network_prefetch_bytes: usize,
        shuffle_max_records: usize,
        shuffle_max_bytes: usize,
        shuffle_seed: u64,
        epoch: u64,
        shard_seed: u64,
        rank: usize,
        world_size: usize,
        worker: usize,
        workers_per_rank: usize,
        encryption_key_id: Option<String>,
        encryption_key: Option<&Bound<'_, PyBytes>>,
    ) -> PyResult<PyBatchLoader> {
        if batch_size == 0 {
            return Err(PyValueError::new_err(
                "batch_size must be greater than zero",
            ));
        }
        let loader = self.records(
            cache_dir,
            cache_max_bytes,
            network_concurrency,
            network_prefetch_bytes,
            shuffle_max_records,
            shuffle_max_bytes,
            shuffle_seed,
            epoch,
            shard_seed,
            rank,
            world_size,
            worker,
            workers_per_rank,
            encryption_key_id,
            encryption_key,
        )?;
        Ok(PyBatchLoader {
            loader,
            batch_size,
            drop_last,
        })
    }
}

fn parse_encryption(
    key_id: Option<String>,
    key: Option<&Bound<'_, PyBytes>>,
) -> PyResult<Option<(String, shardpack_format::EncryptionKey)>> {
    match (key_id, key) {
        (None, None) => Ok(None),
        (Some(key_id), Some(key)) => {
            let bytes: [u8; 32] = key.as_bytes().try_into().map_err(|_| {
                PyValueError::new_err("encryption_key must contain exactly 32 bytes")
            })?;
            Ok(Some((key_id, shardpack_format::EncryptionKey::new(bytes))))
        }
        _ => Err(PyValueError::new_err(
            "encryption_key_id and encryption_key must be provided together",
        )),
    }
}

#[pyclass(unsendable, module = "shardpack._native", name = "RecordLoader")]
struct PyRecordLoader {
    runtime: tokio::runtime::Runtime,
    reader: DatasetReader,
    metrics: RuntimeMetrics,
    cache_metrics: Option<CacheMetrics>,
    shuffle: Option<ShufflePool>,
    source_exhausted: bool,
    checkpoint_requires_encryption: bool,
}

impl PyRecordLoader {
    fn build(
        stores: StoreRegistry,
        layout: Arc<proto::LayoutManifest>,
        options: LoaderOptions,
    ) -> PyResult<Self> {
        let metrics = RuntimeMetrics::default();
        let checkpoint_requires_encryption = layout.encryption.is_some();
        let (reader, cache_metrics) = options.reader(stores, layout, metrics.clone())?;
        let shuffle = match (options.shuffle_max_records, options.shuffle_max_bytes) {
            (0, 0) => None,
            (0, _) | (_, 0) => {
                return Err(PyValueError::new_err(
                    "shuffle_max_records and shuffle_max_bytes must both be zero or non-zero",
                ));
            }
            (max_records, max_bytes) => Some(ShufflePool::new(
                ShuffleLimits {
                    max_records,
                    max_bytes,
                },
                options.shuffle_seed,
            )),
        };
        Ok(Self {
            runtime: loader_runtime(options.network_concurrency)?,
            reader,
            metrics,
            cache_metrics,
            shuffle,
            source_exhausted: false,
            checkpoint_requires_encryption,
        })
    }

    fn next_encoded(&mut self, py: Python<'_>) -> PyResult<Option<EncodedRecord>> {
        loop {
            if self.source_exhausted {
                return Ok(self.shuffle.as_mut().and_then(ShufflePool::drain_next));
            }
            let record = py
                .detach(|| self.runtime.block_on(self.reader.next_record()))
                .map_err(runtime_error)?;
            let Some(record) = record else {
                self.source_exhausted = true;
                continue;
            };
            let Some(shuffle) = &mut self.shuffle else {
                return Ok(Some(record));
            };
            if let Some(record) = shuffle.push(record).map_err(runtime_error)? {
                return Ok(Some(record));
            }
        }
    }

    fn next_encoded_batch(
        &mut self,
        py: Python<'_>,
        max_records: usize,
    ) -> PyResult<Vec<EncodedRecord>> {
        let runtime = &self.runtime;
        let reader = &mut self.reader;
        let shuffle = &mut self.shuffle;
        let source_exhausted = &mut self.source_exhausted;
        py.detach(|| {
            runtime.block_on(async {
                let mut batch = Vec::with_capacity(max_records);
                while batch.len() < max_records {
                    if *source_exhausted {
                        let Some(record) = shuffle.as_mut().and_then(ShufflePool::drain_next)
                        else {
                            break;
                        };
                        batch.push(record);
                        continue;
                    }

                    let Some(record) = reader.next_record().await? else {
                        *source_exhausted = true;
                        continue;
                    };
                    let Some(pool) = shuffle.as_mut() else {
                        batch.push(record);
                        continue;
                    };
                    if let Some(record) = pool.push(record)? {
                        batch.push(record);
                    }
                }
                Ok::<_, shardpack_runtime::Error>(batch)
            })
        })
        .map_err(runtime_error)
    }
}

#[pymethods]
impl PyRecordLoader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyRecord>> {
        self.next_encoded(py)
            .map(|record| record.map(|record| PyRecord::from_encoded(py, record)))
    }

    fn metrics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        metrics_dict(py, self.metrics.snapshot(), self.cache_metrics.as_ref())
    }

    fn checkpoint(&self) -> PyLoaderCheckpoint {
        PyLoaderCheckpoint {
            encoded: encode_checkpoint(
                self.reader.checkpoint(),
                self.shuffle.as_ref().map(ShufflePool::checkpoint),
                self.source_exhausted,
                self.checkpoint_requires_encryption,
            ),
            requires_encryption: self.checkpoint_requires_encryption,
        }
    }
}

#[pyclass(unsendable, module = "shardpack._native", name = "BatchLoader")]
struct PyBatchLoader {
    loader: PyRecordLoader,
    batch_size: usize,
    drop_last: bool,
}

#[pymethods]
impl PyBatchLoader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Vec<PyRecord>>> {
        let encoded = self.loader.next_encoded_batch(py, self.batch_size)?;
        if encoded.is_empty() || (self.drop_last && encoded.len() < self.batch_size) {
            return Ok(None);
        }
        Ok(Some(
            encoded
                .into_iter()
                .map(|record| PyRecord::from_encoded(py, record))
                .collect(),
        ))
    }

    fn metrics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        metrics_dict(
            py,
            self.loader.metrics.snapshot(),
            self.loader.cache_metrics.as_ref(),
        )
    }

    fn checkpoint(&self) -> PyLoaderCheckpoint {
        PyLoaderCheckpoint {
            encoded: encode_checkpoint(
                self.loader.reader.checkpoint(),
                self.loader.shuffle.as_ref().map(ShufflePool::checkpoint),
                self.loader.source_exhausted,
                self.loader.checkpoint_requires_encryption,
            ),
            requires_encryption: self.loader.checkpoint_requires_encryption,
        }
    }
}

fn metrics_dict<'py>(
    py: Python<'py>,
    metrics: shardpack_runtime::RuntimeMetricsSnapshot,
    cache_metrics: Option<&CacheMetrics>,
) -> PyResult<Bound<'py, PyDict>> {
    let result = PyDict::new(py);
    result.set_item("records_read", metrics.records_read)?;
    result.set_item("encoded_bytes_read", metrics.encoded_bytes_read)?;
    result.set_item("shards_completed", metrics.shards_completed)?;
    result.set_item("shard_bytes_completed", metrics.shard_bytes_completed)?;
    result.set_item("shards_verified", metrics.shards_verified)?;
    result.set_item("shard_bytes_verified", metrics.shard_bytes_verified)?;
    result.set_item("prefetched_shards", metrics.prefetched_shards)?;
    result.set_item("retries", metrics.retries)?;
    result.set_item("checksum_failures", metrics.checksum_failures)?;
    result.set_item(
        "queue_backpressure_events",
        metrics.queue_backpressure_events,
    )?;
    if let Some(cache_metrics) = cache_metrics {
        let cache = cache_metrics.snapshot();
        result.set_item("cache_hits", cache.hits)?;
        result.set_item("cache_misses", cache.misses)?;
        result.set_item("cache_downloaded_bytes", cache.downloaded_bytes)?;
        result.set_item("cache_corrupt_entries", cache.corrupt_entries)?;
        result.set_item("cache_evicted_entries", cache.evicted_entries)?;
        result.set_item("cache_evicted_bytes", cache.evicted_bytes)?;
        result.set_item("cache_stale_partials_removed", cache.stale_partials_removed)?;
    }
    Ok(result)
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDataset>()?;
    module.add_class::<PyDatasetWriter>()?;
    module.add_class::<PyLoaderCheckpoint>()?;
    module.add_class::<PyRecord>()?;
    module.add_class::<PyRecordLoader>()?;
    module.add_class::<PyBatchLoader>()?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
