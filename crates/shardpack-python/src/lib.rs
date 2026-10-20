use std::{
    ffi::{c_int, c_void},
    path::PathBuf,
    ptr,
    sync::{Arc, Mutex, mpsc},
    thread,
};

use bytes::Bytes;
use pyo3::{
    buffer::PyBuffer,
    exceptions::{PyBufferError, PyRuntimeError, PyValueError},
    ffi,
    prelude::*,
    types::{PyAny, PyBytes, PyDict, PyMemoryView},
};
use shardpack_cache::{CacheMetrics, LocalCache};
use shardpack_core::{
    EncodedRecord as CoreEncodedRecord, MaterializeEncryption, MaterializeOptions,
    Result as CoreResult, SourceReader, materialize_layout,
};
use shardpack_format::BlockCodec;
use shardpack_manifest::proto;
use shardpack_runtime::{
    DatasetReader, DistributedContext, EncodedRecord, NetworkPrefetch, RuntimeMetrics,
    ShuffleLimits, ShufflePool, pool_shuffle_seed,
};
use shardpack_storage::StoreRegistry;

const DEFAULT_WRITER_MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_WRITER_MAX_BATCH_RECORDS: usize = 1_000_000;

fn load_config(
    config_json: Option<&str>,
    config_path: Option<&std::path::Path>,
) -> PyResult<shardpack_config::ShardPackConfig> {
    match (config_json, config_path) {
        (Some(value), None) => {
            shardpack_config::ShardPackConfig::from_json(value).map_err(runtime_error)
        }
        (None, Some(path)) => shardpack_config::ShardPackConfig::load(path).map_err(runtime_error),
        (Some(_), Some(_)) => Err(PyValueError::new_err(
            "config and config_path are mutually exclusive",
        )),
        (None, None) => Err(PyValueError::new_err(
            "exactly one of config or config_path is required",
        )),
    }
}

#[pyclass(module = "shardpack._native", name = "EpochScheduler")]
struct PyEpochScheduler {
    process_id: u32,
    next_epoch: u64,
    shard_seed: u64,
    shuffle_seed: u64,
}

#[pymethods]
impl PyEpochScheduler {
    #[new]
    #[pyo3(signature = (initial_epoch=0, shard_seed=0, shuffle_seed=1))]
    fn new(initial_epoch: u64, shard_seed: u64, shuffle_seed: u64) -> Self {
        Self {
            process_id: std::process::id(),
            next_epoch: initial_epoch,
            shard_seed,
            shuffle_seed,
        }
    }

    fn next(&mut self) -> PyResult<(u64, u64, u64)> {
        if self.process_id != std::process::id() {
            return Err(PyRuntimeError::new_err(
                "EpochScheduler cannot cross a process boundary; create it inside the worker",
            ));
        }
        let epoch = self.next_epoch;
        self.next_epoch = self
            .next_epoch
            .checked_add(1)
            .ok_or_else(|| PyValueError::new_err("epoch overflow"))?;
        Ok((
            epoch,
            self.shard_seed,
            pool_shuffle_seed(self.shuffle_seed, epoch),
        ))
    }

    fn set_epoch(&mut self, epoch: u64) {
        self.next_epoch = epoch;
    }
}

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
    #[pyo3(signature = (snapshot_id, staging_dir, *, config=None, config_path=None, store=None, target_shard_bytes=1_073_741_824, max_shard_bytes=None, target_block_bytes=16_777_216, block_codec="none", zstd_level=1, object_prefix="shards", layout_prefix="layouts", queue_batches=2, max_batch_records=DEFAULT_WRITER_MAX_BATCH_RECORDS, max_batch_bytes=DEFAULT_WRITER_MAX_BATCH_BYTES, encryption_key_id=None, encryption_key=None, encryption_key_path=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        snapshot_id: &Bound<'_, PyBytes>,
        staging_dir: PathBuf,
        config: Option<String>,
        config_path: Option<PathBuf>,
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
        encryption_key_path: Option<PathBuf>,
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
        let encryption = parse_encryption(encryption_key_id, encryption_key, encryption_key_path)?
            .map(|(key_id, key)| MaterializeEncryption { key_id, key });
        std::fs::create_dir_all(&staging_dir).map_err(runtime_error)?;
        let config = load_config(config.as_deref(), config_path.as_deref())?;
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

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn parse_encryption(
    key_id: Option<String>,
    key: Option<&Bound<'_, PyBytes>>,
    key_path: Option<PathBuf>,
) -> PyResult<Option<(String, shardpack_format::EncryptionKey)>> {
    if key.is_some() && key_path.is_some() {
        return Err(PyValueError::new_err(
            "encryption_key and encryption_key_path are mutually exclusive",
        ));
    }
    let bytes =
        match (key, key_path) {
            (None, None) => None,
            (Some(key), None) => Some(key.as_bytes().try_into().map_err(|_| {
                PyValueError::new_err("encryption_key must contain exactly 32 bytes")
            })?),
            (None, Some(path)) => {
                let value = std::fs::read(&path).map_err(|error| {
                    PyRuntimeError::new_err(format!(
                        "cannot read encryption key file {}: {error}",
                        path.display()
                    ))
                })?;
                Some(value.try_into().map_err(|_| {
                    PyValueError::new_err("encryption key file must contain exactly 32 bytes")
                })?)
            }
            (Some(_), Some(_)) => unreachable!(),
        };
    match (key_id, bytes) {
        (None, None) => Ok(None),
        (Some(key_id), Some(bytes)) => {
            Ok(Some((key_id, shardpack_format::EncryptionKey::new(bytes))))
        }
        _ => Err(PyValueError::new_err(
            "encryption_key_id and one of encryption_key or encryption_key_path must be provided together",
        )),
    }
}

#[pyclass(frozen, module = "shardpack._native", name = "Record")]
struct PyRecord {
    sample_id: [u8; 16],
    flags: u32,
    payload: Bytes,
}

impl PyRecord {
    fn from_encoded(record: EncodedRecord) -> Self {
        Self {
            sample_id: record.sample_id,
            flags: record.flags,
            payload: record.payload,
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
    fn payload<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }

    fn __len__(&self) -> usize {
        self.payload.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "Record(sample_id='{}', flags={}, payload_bytes={})",
            hex::encode(self.sample_id),
            self.flags,
            self.payload.len()
        )
    }

    /// Exports the immutable `Bytes` allocation without copying it into a
    /// Python `bytes`. The buffer owns a reference to this `Record`, so the
    /// allocation remains alive until every derived memoryview is released.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("buffer view is null"));
        }
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            return Err(PyBufferError::new_err("record payload is read-only"));
        }

        let payload = &slf.borrow().payload;
        // SAFETY: `view` was checked for null. `payload` belongs to `slf`, and
        // assigning an owned reference to `view.obj` keeps `slf` alive for the
        // exported buffer lifetime. The format string is static and all shape
        // and stride pointers refer to fields within the stable `Py_buffer`.
        unsafe {
            (*view).obj = slf.clone().into_any().into_ptr();
            (*view).buf = payload.as_ptr().cast_mut().cast::<c_void>();
            (*view).len = payload.len() as isize;
            (*view).readonly = 1;
            (*view).itemsize = 1;
            (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
                c"B".as_ptr().cast_mut()
            } else {
                ptr::null_mut()
            };
            (*view).ndim = 1;
            (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
                &raw mut (*view).len
            } else {
                ptr::null_mut()
            };
            (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
                &raw mut (*view).itemsize
            } else {
                ptr::null_mut()
            };
            (*view).suboffsets = ptr::null_mut();
            (*view).internal = ptr::null_mut();
        }
        Ok(())
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
    #[pyo3(signature = (layout, *, config=None, config_path=None, store=None, expected_snapshot_id=None, expected_layout_id=None))]
    fn new(
        py: Python<'_>,
        layout: String,
        config: Option<String>,
        config_path: Option<PathBuf>,
        store: Option<String>,
        expected_snapshot_id: Option<Vec<u8>>,
        expected_layout_id: Option<Vec<u8>>,
    ) -> PyResult<Self> {
        let identity = match (expected_snapshot_id, expected_layout_id) {
            (Some(snapshot), Some(layout)) => Some(shardpack_core::DatasetIdentity {
                snapshot_id: shardpack_manifest::SnapshotId::from_slice(&snapshot)
                    .map_err(runtime_error)?,
                layout_id: shardpack_manifest::LayoutId::from_slice(&layout)
                    .map_err(runtime_error)?,
            }),
            (None, None) => None,
            _ => {
                return Err(PyValueError::new_err(
                    "expected snapshot and layout IDs must be provided together",
                ));
            }
        };
        let config = load_config(config.as_deref(), config_path.as_deref())?;
        let store_id = config
            .selected_store_id(store.as_deref())
            .map_err(runtime_error)?
            .to_owned();
        let stores = config.build_registry().map_err(runtime_error)?;
        let manifest_store = stores.get(&store_id).map_err(runtime_error)?.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(runtime_error)?;
        let manifest = py
            .detach(|| runtime.block_on(shardpack_core::load_layout(&manifest_store, &layout)))
            .map_err(runtime_error)?;
        if let Some(identity) = identity {
            identity.verify(&manifest).map_err(runtime_error)?;
        }
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
    fn snapshot_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.layout.snapshot_id)
    }

    #[pyo3(signature = (*, shard_seed, world_size, batch_size, drop_last=false))]
    fn validate_epoch(
        &self,
        shard_seed: u64,
        world_size: usize,
        batch_size: u64,
        drop_last: bool,
    ) -> PyResult<Vec<u64>> {
        let plan = shardpack_runtime::EpochPlan::build(
            &self.layout.shards,
            shard_seed,
            world_size,
            batch_size,
            drop_last,
        )
        .map_err(runtime_error)?;
        plan.require_equal_steps().map_err(runtime_error)?;
        if !drop_last
            && plan
                .records_per_rank
                .windows(2)
                .any(|pair| pair[0] != pair[1])
        {
            return Err(PyValueError::new_err(
                "unequal final batch sizes require an explicit gradient weighting policy",
            ));
        }
        Ok(plan.records_per_rank)
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

    #[pyo3(signature = (*, cache_dir=None, cache_max_bytes=274_877_906_944, cache_mode="ciphertext", network_concurrency=2, network_prefetch_bytes=67_108_864, shuffle_max_records=0, shuffle_max_bytes=0, shuffle_seed=1, epoch=0, shard_seed=0, rank=0, world_size=1, worker=0, workers_per_rank=1, encryption_key_id=None, encryption_key=None, encryption_key_path=None))]
    #[allow(clippy::too_many_arguments)]
    fn records(
        &self,
        cache_dir: Option<PathBuf>,
        cache_max_bytes: u64,
        cache_mode: &str,
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
        encryption_key_path: Option<PathBuf>,
    ) -> PyResult<PyRecordLoader> {
        if (shuffle_max_records == 0) != (shuffle_max_bytes == 0) {
            return Err(PyValueError::new_err(
                "shuffle_max_records and shuffle_max_bytes must both be zero or both be non-zero",
            ));
        }
        let encryption = parse_encryption(encryption_key_id, encryption_key, encryption_key_path)?;
        let plaintext_cache = match cache_mode {
            "ciphertext" => false,
            "plaintext" => true,
            _ => {
                return Err(PyValueError::new_err(
                    "cache_mode must be 'ciphertext' or 'plaintext'",
                ));
            }
        };
        if plaintext_cache && cache_dir.is_none() {
            return Err(PyValueError::new_err(
                "cache_mode='plaintext' requires cache_dir",
            ));
        }
        if plaintext_cache && encryption.is_none() {
            return Err(PyValueError::new_err(
                "cache_mode='plaintext' requires encryption_key_id and key material",
            ));
        }

        let metrics = RuntimeMetrics::default();
        let cache_metrics = cache_dir.as_ref().map(|_| CacheMetrics::default());
        let mut reader = DatasetReader::new_assigned_with_registry(
            self.stores.clone(),
            self.layout.clone(),
            epoch,
            shard_seed,
            DistributedContext {
                rank,
                world_size,
                worker,
                workers_per_rank,
            },
        )
        .map_err(runtime_error)?
        .with_metrics(metrics.clone());
        if let Some(cache_dir) = cache_dir {
            let mut cache = LocalCache::new(cache_dir).with_max_bytes(cache_max_bytes);
            if plaintext_cache {
                cache = cache.with_plaintext(
                    encryption
                        .as_ref()
                        .expect("plaintext cache encryption was validated")
                        .1
                        .clone(),
                );
            }
            if let Some(cache_metrics) = &cache_metrics {
                cache = cache.with_metrics(cache_metrics.clone());
            }
            reader = reader.with_cache(cache);
        }
        if let Some((key_id, key)) = encryption {
            reader = reader
                .with_decryption_key(&key_id, key)
                .map_err(runtime_error)?;
        }
        reader = reader
            .with_network_prefetch(NetworkPrefetch {
                concurrency: network_concurrency,
                max_bytes: network_prefetch_bytes,
            })
            .map_err(runtime_error)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(network_concurrency.clamp(1, 4))
            .thread_name("shardpack-python-reader")
            .build()
            .map_err(runtime_error)?;
        Ok(PyRecordLoader {
            runtime,
            reader,
            shuffle: (shuffle_max_records != 0).then(|| {
                ShufflePool::new(
                    ShuffleLimits {
                        max_records: shuffle_max_records,
                        max_bytes: shuffle_max_bytes,
                    },
                    shuffle_seed,
                )
            }),
            source_exhausted: false,
            metrics,
            cache_metrics,
            cancellation: tokio::sync::watch::channel(false).0,
        })
    }
}

#[pyclass(unsendable, module = "shardpack._native", name = "RecordLoader")]
struct PyRecordLoader {
    runtime: tokio::runtime::Runtime,
    reader: DatasetReader,
    shuffle: Option<ShufflePool>,
    source_exhausted: bool,
    metrics: RuntimeMetrics,
    cache_metrics: Option<CacheMetrics>,
    cancellation: tokio::sync::watch::Sender<bool>,
}

impl PyRecordLoader {
    fn next_cancellable(&mut self, py: Python<'_>) -> PyResult<Option<EncodedRecord>> {
        let mut cancelled = self.cancellation.subscribe();
        py.detach(|| {
            self.runtime.block_on(async {
            if *cancelled.borrow() {
                return Err(PyRuntimeError::new_err("record loading cancelled"));
            }
            tokio::select! {
                biased;
                _ = cancelled.changed() => Err(PyRuntimeError::new_err("record loading cancelled")),
                result = self.reader.next_record() => result.map_err(runtime_error),
            }
        })
        })
    }
}

#[pyclass(frozen, module = "shardpack._native", name = "LoaderCancellation")]
struct PyLoaderCancellation {
    sender: tokio::sync::watch::Sender<bool>,
}

#[pymethods]
impl PyLoaderCancellation {
    fn cancel(&self) {
        self.sender.send_replace(true);
    }
}

#[pymethods]
impl PyRecordLoader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyRecord>> {
        if *self.cancellation.borrow() {
            return Err(PyRuntimeError::new_err("record loading cancelled"));
        }
        if self.shuffle.is_none() {
            let record = self.next_cancellable(py)?;
            return Ok(record.map(PyRecord::from_encoded));
        }

        loop {
            if self.source_exhausted {
                return Ok(self
                    .shuffle
                    .as_mut()
                    .expect("shuffle pool exists")
                    .drain_next()
                    .map(PyRecord::from_encoded));
            }
            let record = self.next_cancellable(py)?;
            match record {
                Some(record) => {
                    if let Some(emitted) = self
                        .shuffle
                        .as_mut()
                        .expect("shuffle pool exists")
                        .push(record)
                        .map_err(runtime_error)?
                    {
                        return Ok(Some(PyRecord::from_encoded(emitted)));
                    }
                }
                None => self.source_exhausted = true,
            }
        }
    }

    fn metrics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.metrics_handle().snapshot(py)
    }

    fn metrics_handle(&self) -> PyLoaderMetrics {
        PyLoaderMetrics {
            metrics: self.metrics.clone(),
            cache_metrics: self.cache_metrics.clone(),
        }
    }

    fn cancellation_handle(&self) -> PyLoaderCancellation {
        PyLoaderCancellation {
            sender: self.cancellation.clone(),
        }
    }
}

/// Share only atomic counters, never the thread-affine reader or its runtime.
#[pyclass(frozen, module = "shardpack._native", name = "LoaderMetrics")]
struct PyLoaderMetrics {
    metrics: RuntimeMetrics,
    cache_metrics: Option<CacheMetrics>,
}

#[pymethods]
impl PyLoaderMetrics {
    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let metrics = self.metrics.snapshot();
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
        if let Some(cache_metrics) = &self.cache_metrics {
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
}

#[pyfunction]
fn cli_main(py: Python<'_>, args: Vec<String>) -> i32 {
    py.detach(|| shardpack_cli::main_from(args))
}

#[pyclass(module = "shardpack._native", name = "SamplePool")]
struct PySamplePool {
    pool: shardpack_runtime::SamplePool,
}

fn pool_exclusions(ids: Option<Vec<Vec<u8>>>) -> PyResult<Vec<[u8; 16]>> {
    ids.unwrap_or_default()
        .into_iter()
        .map(|id| {
            id.try_into().map_err(|_| {
                PyValueError::new_err("excluded SampleId must contain exactly 16 bytes")
            })
        })
        .collect()
}

#[pymethods]
impl PySamplePool {
    #[new]
    #[pyo3(signature = (*, max_records, max_bytes, seed=0))]
    fn new(max_records: usize, max_bytes: usize, seed: u64) -> PyResult<Self> {
        let pool = shardpack_runtime::SamplePool::new(
            shardpack_runtime::SamplePoolLimits {
                max_records,
                max_bytes,
            },
            seed,
        )
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self { pool })
    }

    fn insert(&mut self, record: &PyRecord) -> PyResult<()> {
        self.pool
            .insert(EncodedRecord {
                sample_id: record.sample_id,
                flags: record.flags,
                payload: record.payload.clone(),
            })
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    #[pyo3(signature = (count, *, exclude_ids=None, replace=false))]
    fn sample(
        &mut self,
        count: usize,
        exclude_ids: Option<Vec<Vec<u8>>>,
        replace: bool,
    ) -> PyResult<Vec<PyRecord>> {
        self.pool
            .sample(count, &pool_exclusions(exclude_ids)?, replace)
            .map(|records| records.into_iter().map(PyRecord::from_encoded).collect())
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    #[pyo3(signature = (*, exclude_ids=None))]
    fn available(&self, exclude_ids: Option<Vec<Vec<u8>>>) -> PyResult<usize> {
        Ok(self.pool.available(&pool_exclusions(exclude_ids)?))
    }

    #[pyo3(signature = (*, seed=0))]
    fn reset(&mut self, seed: u64) {
        self.pool.reset(seed);
    }

    fn __len__(&self) -> usize {
        self.pool.len()
    }

    #[getter]
    fn buffered_bytes(&self) -> usize {
        self.pool.buffered_bytes()
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDataset>()?;
    module.add_class::<PyDatasetWriter>()?;
    module.add_class::<PyRecord>()?;
    module.add_class::<PyRecordLoader>()?;
    module.add_class::<PyLoaderMetrics>()?;
    module.add_class::<PyLoaderCancellation>()?;
    module.add_class::<PyEpochScheduler>()?;
    module.add_class::<PySamplePool>()?;
    module.add_function(wrap_pyfunction!(cli_main, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
