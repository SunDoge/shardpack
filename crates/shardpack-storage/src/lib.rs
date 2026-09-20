//! Apache OpenDAL-backed object access for local tests and S3 production storage.

use std::{ops::Range, path::Path, pin::Pin};

use bytes::Bytes;
use futures_util::{SinkExt, Stream, TryStreamExt};
use opendal::{
    ErrorKind, Operator,
    services::{Fs, S3},
};
use snafu::Snafu;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;
use tokio_util::io::StreamReader;

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + 'static>>;
pub type AsyncObjectReader = Pin<Box<dyn AsyncRead + Send + 'static>>;

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
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug)]
pub struct Store {
    operator: Operator,
    write_chunk_bytes: Option<usize>,
    write_concurrency: usize,
}

/// Explicit S3 addressing options. Credentials are resolved by OpenDAL's AWS
/// provider chain unless supplied here, so the same type works with AWS, MinIO
/// and other S3-compatible services.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Options {
    pub bucket: String,
    pub root: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub write_chunk_bytes: usize,
    pub write_concurrency: usize,
}

impl Default for S3Options {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            root: None,
            endpoint: None,
            region: None,
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
            write_chunk_bytes: None,
            write_concurrency: 1,
        })
    }

    pub fn filesystem(root: &Path) -> Result<Self> {
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
        Self::new(operator)
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

    pub async fn delete(&self, key: &str) -> Result<()> {
        validate_key(key)?;
        self.operator
            .delete(key)
            .await
            .map_err(|source| map_backend("delete", key, source))
    }
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
    use crate::{Error, S3Options, Store};
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

    #[test]
    fn s3_backend_advertises_required_immutable_create_capability() {
        let store = Store::s3(&S3Options {
            bucket: "configuration-check".into(),
            endpoint: Some("http://127.0.0.1:9000".into()),
            region: Some("us-east-1".into()),
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
