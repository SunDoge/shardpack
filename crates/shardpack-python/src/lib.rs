use std::{
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    thread,
};

use bytes::Bytes;
use pyo3::{
    buffer::PyBuffer,
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::{PyAny, PyBytes},
};
use shardpack_core::{
    EncodedRecord as CoreEncodedRecord, MaterializeEncryption, MaterializeOptions,
    Result as CoreResult, SourceReader, materialize_layout,
};
use shardpack_format::BlockCodec;
use shardpack_manifest::proto;
use shardpack_runtime::{DatasetReader, EncodedRecord};
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

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
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
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(runtime_error)?;
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

    #[pyo3(signature = (*, encryption_key_id=None, encryption_key=None))]
    fn records(
        &self,
        encryption_key_id: Option<String>,
        encryption_key: Option<&Bound<'_, PyBytes>>,
    ) -> PyResult<PyRecordLoader> {
        let mut reader = DatasetReader::new_with_registry(self.stores.clone(), self.layout.clone())
            .map_err(runtime_error)?;
        if let Some((key_id, key)) = parse_encryption(encryption_key_id, encryption_key)? {
            reader = reader
                .with_decryption_key(&key_id, key)
                .map_err(runtime_error)?;
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(runtime_error)?;
        Ok(PyRecordLoader { runtime, reader })
    }
}

#[pyclass(unsendable, module = "shardpack._native", name = "RecordLoader")]
struct PyRecordLoader {
    runtime: tokio::runtime::Runtime,
    reader: DatasetReader,
}

#[pymethods]
impl PyRecordLoader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyRecord>> {
        let record = py
            .detach(|| self.runtime.block_on(self.reader.next_record()))
            .map_err(runtime_error)?;
        Ok(record.map(|record| PyRecord::from_encoded(py, record)))
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDataset>()?;
    module.add_class::<PyDatasetWriter>()?;
    module.add_class::<PyRecord>()?;
    module.add_class::<PyRecordLoader>()?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
