//! Apache OpenDAL-backed object access for local tests and S3 production storage.

use std::{
    collections::HashMap,
    ops::Range,
    path::{Path, PathBuf},
    pin::Pin,
};

use bytes::Bytes;
use futures_util::{SinkExt, Stream, TryStreamExt};
use opendal::{
    ErrorKind, Operator,
    services::{Fs, S3},
};
use snafu::Snafu;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::ReaderStream;
use tokio_util::io::StreamReader;

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + 'static>>;
pub type AsyncObjectReader = Pin<Box<dyn AsyncRead + Send + 'static>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TempfilePublication {
    /// The staging inode became the immutable destination without copying.
    AtomicFilesystem,
    /// Bytes were streamed through the object-store backend.
    Streamed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: String,
    pub byte_size: u64,
    pub last_modified_unix_seconds: Option<i64>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("invalid object key {key:?}: {reason}"))]
    InvalidKey { key: String, reason: &'static str },
    #[snafu(display("object {key:?} not found"))]
    NotFound { key: String },
    #[snafu(display("immutable object {key:?} already exists"))]
    AlreadyExists { key: String },
    #[snafu(display("storage backend {scheme} cannot conditionally create immutable objects"))]
    ImmutableWriteUnsupported { scheme: String },
    #[snafu(display("OpenDAL {operation} failed for {key:?}: {source}"))]
    Backend {
        operation: &'static str,
        key: String,
        source: Box<opendal::Error>,
    },
    #[snafu(display("OpenDAL streaming {operation} failed for {key:?}: {source}"))]
    StreamIo {
        operation: &'static str,
        key: String,
        source: std::io::Error,
    },
    #[snafu(display("storage registry already contains store {store_id:?}"))]
    DuplicateStore { store_id: String },
    #[snafu(display("storage registry has no store named {store_id:?}"))]
    UnknownStore { store_id: String },
    #[snafu(display("object {key:?} exceeds read limit of {limit} bytes"))]
    ReadLimitExceeded { key: String, limit: usize },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct Store {
    operator: Operator,
    filesystem_root: Option<PathBuf>,
    write_chunk_bytes: Option<usize>,
    write_concurrency: usize,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Store")
            .field("scheme", &self.operator.info().scheme())
            .field("filesystem_fast_publish", &self.filesystem_root.is_some())
            .field("write_chunk_bytes", &self.write_chunk_bytes)
            .field("write_concurrency", &self.write_concurrency)
            .finish()
    }
}

/// Explicit S3 addressing options. Credentials are resolved by OpenDAL's AWS
/// provider chain unless supplied here, so the same type works with AWS, MinIO
/// and other S3-compatible services.
#[derive(Clone, PartialEq, Eq)]
pub struct S3Options {
    pub bucket: String,
    pub root: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub write_chunk_bytes: usize,
    pub write_concurrency: usize,
}

impl std::fmt::Debug for S3Options {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3Options")
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("profile", &self.profile)
            .field(
                "access_key_id",
                &self.access_key_id.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("write_chunk_bytes", &self.write_chunk_bytes)
            .field("write_concurrency", &self.write_concurrency)
            .finish()
    }
}

impl Default for S3Options {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            root: None,
            endpoint: None,
            region: None,
            profile: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            write_chunk_bytes: 16 * 1024 * 1024,
            write_concurrency: 8,
        }
    }
}

impl Store {
    pub fn new(operator: Operator) -> Result<Self> {
        let info = operator.info();
        if !info.capability().write_with_if_not_exists {
            return Err(Error::ImmutableWriteUnsupported {
                scheme: info.scheme().to_string(),
            });
        }
        Ok(Self {
            operator,
            filesystem_root: None,
            write_chunk_bytes: None,
            write_concurrency: 1,
        })
    }

    pub fn filesystem(root: &Path) -> Result<Self> {
        let root_path = root.to_path_buf();
        let root = root.to_str().ok_or_else(|| Error::InvalidKey {
            key: root.display().to_string(),
            reason: "filesystem root must be valid UTF-8",
        })?;
        let builder = Fs::default().root(root).atomic_write_dir(root);
        let operator = Operator::new(builder).map_err(|source| Error::Backend {
            operation: "initialize filesystem",
            key: root.to_owned(),
            source: Box::new(source),
        })?;
        let mut store = Self::new(operator)?;
        store.filesystem_root = Some(root_path);
        Ok(store)
    }

    pub fn s3(options: &S3Options) -> Result<Self> {
        // OpenDAL 0.58 no longer installs an HTTP transport when default
        // features are disabled. Installation is process-wide, idempotent and
        // required before the first S3 request.
        opendal::install_default();
        if options.bucket.is_empty() {
            return Err(Error::InvalidKey {
                key: options.bucket.clone(),
                reason: "S3 bucket must not be empty",
            });
        }
        if options.write_chunk_bytes < 5 * 1024 * 1024 {
            return Err(Error::InvalidKey {
                key: options.write_chunk_bytes.to_string(),
                reason: "S3 write chunk must be at least 5 MiB",
            });
        }
        if options.write_concurrency == 0 {
            return Err(Error::InvalidKey {
                key: options.write_concurrency.to_string(),
                reason: "S3 write concurrency must be greater than zero",
            });
        }
        let mut builder = S3::default().bucket(&options.bucket);
        if let Some(value) = &options.root {
            builder = builder.root(value);
        }
        if let Some(value) = &options.endpoint {
            builder = builder.endpoint(value);
        }
        if let Some(value) = &options.region {
            builder = builder.region(value);
        }
        if let Some(value) = &options.profile {
            builder = builder.profile(value);
        }
        if let Some(value) = &options.access_key_id {
            builder = builder.access_key_id(value);
        }
        if let Some(value) = &options.secret_access_key {
            builder = builder.secret_access_key(value);
        }
        if let Some(value) = &options.session_token {
            builder = builder.session_token(value);
        }
        let operator = Operator::new(builder).map_err(|source| Error::Backend {
            operation: "initialize S3",
            key: options.bucket.clone(),
            source: Box::new(source),
        })?;
        let mut store = Self::new(operator)?;
        store.write_chunk_bytes = Some(options.write_chunk_bytes);
        store.write_concurrency = options.write_concurrency;
        Ok(store)
    }

    #[must_use]
    pub const fn operator(&self) -> &Operator {
        &self.operator
    }

    pub async fn stream(&self, key: &str) -> Result<ByteStream> {
        validate_key(key)?;
        let reader = self
            .operator
            .reader(key)
            .await
            .map_err(|source| map_backend("open reader", key, source))?;
        let stream = reader
            .into_bytes_stream(..)
            .await
            .map_err(|source| map_backend("start stream", key, source))?
            .map_err({
                let key = key.to_owned();
                move |source| Error::StreamIo {
                    operation: "read",
                    key: key.clone(),
                    source,
                }
            });
        Ok(Box::pin(stream))
    }

    pub async fn open_reader(&self, key: &str) -> Result<AsyncObjectReader> {
        self.open_reader_from(key, 0).await
    }

    /// Opens a sequential stream beginning at `offset` using an object-store
    /// range request. The returned reader does not expose seeking.
    pub async fn open_reader_from(&self, key: &str, offset: u64) -> Result<AsyncObjectReader> {
        validate_key(key)?;
        let reader = self
            .operator
            .reader(key)
            .await
            .map_err(|source| map_backend("open reader", key, source))?;
        let stream = reader
            .into_bytes_stream(offset..)
            .await
            .map_err(|source| map_backend("start stream", key, source))?;
        Ok(Box::pin(StreamReader::new(stream)))
    }

    pub async fn read_range(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        validate_key(key)?;
        self.operator
            .read_with(key)
            .range(range)
            .await
            .map(|buffer| buffer.to_bytes())
            .map_err(|source| map_backend("range read", key, source))
    }

    pub async fn read_all(&self, key: &str) -> Result<Bytes> {
        validate_key(key)?;
        self.operator
            .read(key)
            .await
            .map(|buffer| buffer.to_bytes())
            .map_err(|source| map_backend("read", key, source))
    }

    /// Reads an object while enforcing a hard allocation bound. At most one
    /// byte beyond `limit` is consumed to distinguish an exact-size object
    /// from an oversized one.
    pub async fn read_all_limited(&self, key: &str, limit: usize) -> Result<Bytes> {
        let probe_limit = limit
            .checked_add(1)
            .ok_or_else(|| Error::ReadLimitExceeded {
                key: key.to_owned(),
                limit,
            })?;
        let probe_limit = u64::try_from(probe_limit).map_err(|_| Error::ReadLimitExceeded {
            key: key.to_owned(),
            limit,
        })?;
        let mut reader = self.open_reader(key).await?.take(probe_limit);
        let mut bytes = Vec::with_capacity(limit.min(1024 * 1024));
        reader
            .read_to_end(&mut bytes)
            .await
            .map_err(|source| Error::StreamIo {
                operation: "bounded read",
                key: key.to_owned(),
                source,
            })?;
        if bytes.len() > limit {
            return Err(Error::ReadLimitExceeded {
                key: key.to_owned(),
                limit,
            });
        }
        Ok(Bytes::from(bytes))
    }

    /// Recursively lists object keys below a normalized relative prefix.
    pub async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .list_objects(prefix)
            .await?
            .into_iter()
            .map(|object| object.key)
            .collect())
    }

    /// Recursively lists object keys and the metadata supplied by the backend.
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        validate_prefix(prefix)?;
        let mut objects = self
            .operator
            .list_with(prefix)
            .recursive(true)
            .await
            .map_err(|source| map_backend("list", prefix, source))?
            .into_iter()
            .filter(|entry| entry.metadata().is_file())
            .map(|entry| ObjectInfo {
                key: entry.path().to_owned(),
                byte_size: entry.metadata().content_length(),
                last_modified_unix_seconds: entry
                    .metadata()
                    .last_modified()
                    .map(|value| value.into_inner().as_second()),
            })
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(objects)
    }

    pub async fn write_immutable(&self, key: &str, mut input: ByteStream) -> Result<()> {
        validate_key(key)?;
        let mut writer = self.operator.writer_with(key).if_not_exists(true);
        if let Some(chunk_bytes) = self.write_chunk_bytes {
            writer = writer.chunk(chunk_bytes).concurrent(self.write_concurrency);
        }
        let writer = writer
            .await
            .map_err(|source| map_write_error("open writer", key, source))?;
        let mut sink = writer.into_bytes_sink();
        while let Some(chunk) = input.try_next().await? {
            sink.send(chunk)
                .await
                .map_err(|source| map_stream_write_error("write", key, source))?;
        }
        sink.close()
            .await
            .map_err(|source| map_stream_write_error("finalize write", key, source))
    }

    pub async fn write_bytes_immutable(&self, key: &str, bytes: Bytes) -> Result<()> {
        self.write_immutable(
            key,
            Box::pin(futures_util::stream::once(async move { Ok(bytes) })),
        )
        .await
    }

    pub async fn write_file_immutable(&self, key: &str, path: &Path) -> Result<()> {
        validate_key(key)?;
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|source| Error::StreamIo {
                operation: "open staging file",
                key: key.to_owned(),
                source,
            })?;
        let stream = ReaderStream::new(file).map_err({
            let key = key.to_owned();
            move |source| Error::StreamIo {
                operation: "read staging file",
                key: key.clone(),
                source,
            }
        });
        self.write_immutable(key, Box::pin(stream)).await
    }

    /// Publishes a completed staging file without copying when the filesystem
    /// backend and staging directory share a filesystem. Ownership is consumed
    /// so no writable staging handle survives a successful publication.
    pub async fn publish_tempfile_immutable(
        &self,
        key: &str,
        staging: tempfile::NamedTempFile,
    ) -> Result<TempfilePublication> {
        validate_key(key)?;
        let Some(root) = &self.filesystem_root else {
            self.write_file_immutable(key, staging.path()).await?;
            return Ok(TempfilePublication::Streamed);
        };
        let target = root.join(key);
        let parent = target.parent().expect("validated key has a parent");
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| Error::StreamIo {
                operation: "create object parent directory",
                key: key.to_owned(),
                source,
            })?;
        match staging.persist_noclobber(&target) {
            Ok(file) => {
                drop(file);
                Ok(TempfilePublication::AtomicFilesystem)
            }
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(Error::AlreadyExists {
                    key: key.to_owned(),
                })
            }
            Err(error) if error.error.kind() == std::io::ErrorKind::CrossesDevices => {
                self.write_file_immutable(key, error.file.path()).await?;
                Ok(TempfilePublication::Streamed)
            }
            Err(error) => Err(Error::StreamIo {
                operation: "atomically publish staging file",
                key: key.to_owned(),
                source: error.error,
            }),
        }
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        validate_key(key)?;
        self.operator
            .delete(key)
            .await
            .map_err(|source| map_backend("delete", key, source))
    }
}

/// Named object stores used to resolve physical locations in a layout.
/// Store IDs are deployment configuration, while object keys remain immutable
/// layout data.
#[derive(Clone, Default)]
pub struct StoreRegistry {
    stores: HashMap<String, Store>,
}

impl std::fmt::Debug for StoreRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut store_ids = self.stores.keys().collect::<Vec<_>>();
        store_ids.sort();
        formatter
            .debug_struct("StoreRegistry")
            .field("store_ids", &store_ids)
            .finish()
    }
}

impl StoreRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, store_id: impl Into<String>, store: Store) -> Result<()> {
        let store_id = store_id.into();
        validate_store_id(&store_id)?;
        if self.stores.insert(store_id.clone(), store).is_some() {
            return Err(Error::DuplicateStore { store_id });
        }
        Ok(())
    }

    pub fn get(&self, store_id: &str) -> Result<&Store> {
        self.stores
            .get(store_id)
            .ok_or_else(|| Error::UnknownStore {
                store_id: store_id.to_owned(),
            })
    }

    #[must_use]
    pub fn contains(&self, store_id: &str) -> bool {
        self.stores.contains_key(store_id)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.stores.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }
}

fn validate_store_id(store_id: &str) -> Result<()> {
    if store_id.is_empty()
        || !store_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(Error::InvalidKey {
            key: store_id.to_owned(),
            reason: "store ID must contain only ASCII letters, digits, '-' or '_'",
        });
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.starts_with('/')
        || key
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        Err(Error::InvalidKey {
            key: key.to_owned(),
            reason: "must be a normalized, non-empty relative object key",
        })
    } else {
        Ok(())
    }
}

fn validate_prefix(prefix: &str) -> Result<()> {
    let trimmed = prefix.strip_suffix('/').unwrap_or(prefix);
    validate_key(trimmed)
}

fn map_backend(operation: &'static str, key: &str, source: opendal::Error) -> Error {
    if source.kind() == ErrorKind::NotFound {
        Error::NotFound {
            key: key.to_owned(),
        }
    } else {
        Error::Backend {
            operation,
            key: key.to_owned(),
            source: Box::new(source),
        }
    }
}

fn map_write_error(operation: &'static str, key: &str, source: opendal::Error) -> Error {
    if source.kind() == ErrorKind::ConditionNotMatch || source.kind() == ErrorKind::AlreadyExists {
        Error::AlreadyExists {
            key: key.to_owned(),
        }
    } else {
        map_backend(operation, key, source)
    }
}

fn map_stream_write_error(operation: &'static str, key: &str, source: std::io::Error) -> Error {
    if source
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<opendal::Error>())
        .is_some_and(|error| {
            error.kind() == ErrorKind::ConditionNotMatch || error.kind() == ErrorKind::AlreadyExists
        })
    {
        Error::AlreadyExists {
            key: key.to_owned(),
        }
    } else {
        Error::StreamIo {
            operation,
            key: key.to_owned(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Error, S3Options, Store, StoreRegistry, TempfilePublication};
    use bytes::Bytes;
    use futures_util::{StreamExt, stream};

    #[tokio::test]
    async fn filesystem_uses_same_streaming_api_as_s3() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::filesystem(directory.path()).unwrap();
        let input = stream::iter([
            Ok(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"def")),
        ]);
        store
            .write_immutable("ab/cd/object.spk", Box::pin(input))
            .await
            .unwrap();
        assert_eq!(
            store.read_range("ab/cd/object.spk", 1..5).await.unwrap(),
            "bcde"
        );
        let mut ranged = store.open_reader_from("ab/cd/object.spk", 2).await.unwrap();
        let mut ranged_bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut ranged, &mut ranged_bytes)
            .await
            .unwrap();
        assert_eq!(ranged_bytes, b"cdef");
        assert_eq!(
            store.read_all_limited("ab/cd/object.spk", 6).await.unwrap(),
            "abcdef"
        );
        assert!(matches!(
            store.read_all_limited("ab/cd/object.spk", 5).await,
            Err(Error::ReadLimitExceeded { limit: 5, .. })
        ));
        let chunks: Vec<_> = store
            .stream("ab/cd/object.spk")
            .await
            .unwrap()
            .collect()
            .await;
        assert_eq!(
            chunks
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .concat(),
            b"abcdef"
        );
        let duplicate = stream::iter([Ok(Bytes::from_static(b"replacement"))]);
        assert!(matches!(
            store
                .write_immutable("ab/cd/object.spk", Box::pin(duplicate))
                .await,
            Err(Error::AlreadyExists { .. })
        ));
    }

    #[tokio::test]
    async fn filesystem_atomically_consumes_and_publishes_staging_file_without_copying() {
        let directory = tempfile::tempdir().unwrap();
        let staging = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
        std::fs::write(staging.path(), b"immutable shard bytes").unwrap();
        let store = Store::filesystem(directory.path()).unwrap();

        let publication = store
            .publish_tempfile_immutable("shards/ab/object.spk", staging)
            .await
            .unwrap();
        assert_eq!(publication, TempfilePublication::AtomicFilesystem);
        let published = directory.path().join("shards/ab/object.spk");
        assert_eq!(std::fs::read(&published).unwrap(), b"immutable shard bytes");
        let duplicate = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
        std::fs::write(duplicate.path(), b"changed staging bytes").unwrap();
        assert!(matches!(
            store
                .publish_tempfile_immutable("shards/ab/object.spk", duplicate)
                .await,
            Err(Error::AlreadyExists { .. })
        ));
        assert_eq!(std::fs::read(published).unwrap(), b"immutable shard bytes");
    }

    #[test]
    fn registry_rejects_duplicate_and_unknown_stores() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::filesystem(directory.path()).unwrap();
        let mut registry = StoreRegistry::new();
        registry.insert("primary", store.clone()).unwrap();
        assert!(matches!(
            registry.insert("primary", store),
            Err(Error::DuplicateStore { .. })
        ));
        assert!(matches!(
            registry.get("missing"),
            Err(Error::UnknownStore { .. })
        ));
    }

    #[test]
    fn s3_options_debug_redacts_credentials() {
        let options = S3Options {
            access_key_id: Some("access-id".into()),
            secret_access_key: Some("secret-value".into()),
            session_token: Some("session-value".into()),
            ..S3Options::default()
        };
        let debug = format!("{options:?}");
        assert!(!debug.contains("access-id"));
        assert!(!debug.contains("secret-value"));
        assert!(!debug.contains("session-value"));
    }

    #[test]
    fn s3_backend_advertises_required_immutable_create_capability() {
        let store = Store::s3(&S3Options {
            bucket: "configuration-check".into(),
            endpoint: Some("http://127.0.0.1:9000".into()),
            region: Some("us-east-1".into()),
            profile: None,
            access_key_id: Some("test".into()),
            secret_access_key: Some("test".into()),
            ..S3Options::default()
        })
        .unwrap();
        assert_eq!(store.operator().info().scheme().to_string(), "s3");
        assert!(
            store
                .operator()
                .info()
                .capability()
                .write_with_if_not_exists
        );
    }
}
