//! Verified local cache keyed by immutable `ShardId`.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_stream::try_stream;
use bytes::Bytes;
use futures_util::TryStreamExt;
use shardpack_manifest::ShardId;
use shardpack_storage::{AsyncObjectReader, Store};
use snafu::Snafu;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::io::StreamReader;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const DOWNLOAD_LOCK_DIRECTORY: &str = ".shardpack-download-locks";
const PLAINTEXT_META_MAGIC: &[u8; 8] = b"SPKPCHE\0";
const PLAINTEXT_META_SIZE: usize = 120;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CacheMode {
    #[default]
    Ciphertext,
    Plaintext,
}

pub struct CacheObject {
    pub reader: AsyncObjectReader,
    pub plaintext: bool,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("storage read failed: {source}"))]
    Storage { source: shardpack_storage::Error },
    #[snafu(display("cache I/O operation {operation} failed for {path}: {source}"))]
    Io {
        operation: &'static str,
        path: String,
        source: std::io::Error,
    },
    #[snafu(display("cached or downloaded shard {shard_id} failed identity verification"))]
    IdentityMismatch { shard_id: ShardId },
    #[snafu(display("ShardPack conversion failed: {source}"))]
    Format { source: shardpack_format::Error },
    #[snafu(display("plaintext cache metadata is invalid for shard {shard_id}"))]
    InvalidPlaintextMetadata { shard_id: ShardId },
    #[snafu(display("plaintext cache for shard {shard_id} was created with a different key"))]
    PlaintextKeyMismatch { shard_id: ShardId },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug)]
pub struct LocalCache {
    root: PathBuf,
    max_bytes: Option<u64>,
    metrics: CacheMetrics,
    mode: CacheMode,
    decryption_key: Option<shardpack_format::EncryptionKey>,
}

#[derive(Clone, Debug, Default)]
pub struct CacheMetrics(Arc<CacheMetricsInner>);

#[derive(Debug, Default)]
struct CacheMetricsInner {
    hits: AtomicU64,
    misses: AtomicU64,
    downloaded_bytes: AtomicU64,
    corrupt_entries: AtomicU64,
    evicted_entries: AtomicU64,
    evicted_bytes: AtomicU64,
    stale_partials_removed: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheMetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub downloaded_bytes: u64,
    pub corrupt_entries: u64,
    pub evicted_entries: u64,
    pub evicted_bytes: u64,
    pub stale_partials_removed: u64,
}

impl CacheMetrics {
    #[must_use]
    pub fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            hits: self.0.hits.load(Ordering::Relaxed),
            misses: self.0.misses.load(Ordering::Relaxed),
            downloaded_bytes: self.0.downloaded_bytes.load(Ordering::Relaxed),
            corrupt_entries: self.0.corrupt_entries.load(Ordering::Relaxed),
            evicted_entries: self.0.evicted_entries.load(Ordering::Relaxed),
            evicted_bytes: self.0.evicted_bytes.load(Ordering::Relaxed),
            stale_partials_removed: self.0.stale_partials_removed.load(Ordering::Relaxed),
        }
    }
}

impl LocalCache {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_bytes: None,
            metrics: CacheMetrics::default(),
            mode: CacheMode::Ciphertext,
            decryption_key: None,
        }
    }

    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: CacheMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    #[must_use]
    pub fn with_plaintext(mut self, key: shardpack_format::EncryptionKey) -> Self {
        self.mode = CacheMode::Plaintext;
        self.decryption_key = Some(key);
        self
    }

    #[must_use]
    pub const fn mode(&self) -> CacheMode {
        self.mode
    }

    #[must_use]
    pub fn metrics(&self) -> CacheMetrics {
        self.metrics.clone()
    }

    pub async fn open(
        &self,
        store: &Store,
        object_key: &str,
        shard_id: ShardId,
        expected_bytes: u64,
    ) -> Result<AsyncObjectReader> {
        Ok(self
            .open_object(store, object_key, shard_id, expected_bytes)
            .await?
            .reader)
    }

    pub async fn open_object(
        &self,
        store: &Store,
        object_key: &str,
        shard_id: ShardId,
        expected_bytes: u64,
    ) -> Result<CacheObject> {
        if self.mode == CacheMode::Plaintext {
            return self
                .open_plaintext(store, object_key, shard_id, expected_bytes)
                .await;
        }
        let final_path = self.path_for(shard_id);
        if let Some(reader) = open_file_if_expected_size(&final_path, expected_bytes).await? {
            self.metrics.0.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(CacheObject {
                reader,
                plaintext: false,
            });
        }

        let parent = final_path.parent().expect("cache entry has parent");
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| io_error("create cache directory", parent, source))?;
        let download_lock = CacheDownloadLock::acquire(&self.root, shard_id).await?;

        // Another process may have completed the same immutable download while
        // this opener waited for the per-shard lock.
        if let Some(reader) = open_file_if_expected_size(&final_path, expected_bytes).await? {
            self.metrics.0.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(CacheObject {
                reader,
                plaintext: false,
            });
        }
        let existed = tokio::fs::try_exists(&final_path)
            .await
            .map_err(|source| io_error("stat", &final_path, source))?;
        if existed {
            self.metrics
                .0
                .corrupt_entries
                .fetch_add(1, Ordering::Relaxed);
            let _lock = CacheMutationLock::acquire(&self.root).await?;
            match tokio::fs::remove_file(&final_path).await {
                Ok(()) => {}
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(io_error("remove corrupt entry", &final_path, source));
                }
            }
        }

        self.metrics.0.misses.fetch_add(1, Ordering::Relaxed);

        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary =
            final_path.with_extension(format!("spk.tmp-{}-{sequence}", std::process::id()));
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|source| io_error("create temporary entry", &temporary, source))?;
        let mut remote = store
            .stream(object_key)
            .await
            .map_err(|source| Error::Storage { source })?;
        let cache_root = self.root.clone();
        let max_bytes = self.max_bytes;
        let metrics = self.metrics.clone();
        let stream = try_stream! {
            let _download_lock = download_lock;
            let mut guard = TemporaryGuard(Some(temporary.clone()));
            let mut file = file;
            let mut hasher = blake3::Hasher::new();
            let mut byte_size = 0_u64;
            while let Some(chunk) = remote.try_next().await.map_err(|source| Error::Storage { source })? {
                file.write_all(&chunk).await.map_err(|source| io_error("write temporary entry", &temporary, source))?;
                hasher.update(&chunk);
                byte_size = byte_size.saturating_add(chunk.len() as u64);
                metrics.0.downloaded_bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                yield chunk;
            }
            file.sync_all().await.map_err(|source| io_error("sync temporary entry", &temporary, source))?;
            drop(file);
            if byte_size != expected_bytes || hasher.finalize().as_bytes() != shard_id.as_bytes() {
                Err(Error::IdentityMismatch { shard_id })?;
            }
            let _lock = CacheMutationLock::acquire(&cache_root).await?;
            match tokio::fs::hard_link(&temporary, &final_path).await {
                Ok(()) => {}
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                    if !verify_file(&final_path, shard_id, expected_bytes).await? {
                        Err(Error::IdentityMismatch { shard_id })?;
                    }
                }
                Err(source) => Err(io_error("publish cache entry", &final_path, source))?,
            }
            tokio::fs::remove_file(&temporary).await
                .map_err(|source| io_error("remove temporary entry", &temporary, source))?;
            guard.0 = None;
            if let Some(limit) = max_bytes {
                evict_to_limit(&cache_root, limit, &final_path, &metrics).await?;
            }
        };
        Ok(CacheObject {
            reader: Box::pin(StreamReader::new(
                stream.map_err(|error: Error| std::io::Error::other(error)),
            )),
            plaintext: false,
        })
    }

    async fn open_plaintext(
        &self,
        store: &Store,
        object_key: &str,
        shard_id: ShardId,
        expected_ciphertext_bytes: u64,
    ) -> Result<CacheObject> {
        let key = self
            .decryption_key
            .clone()
            .expect("plaintext cache mode always contains a decryption key");
        let final_path = self.path_for(shard_id);
        let meta_path = plaintext_meta_path(&final_path);
        let key_commitment = key.cache_commitment();
        if let Some((reader, meta)) =
            open_plaintext_hit(&final_path, &meta_path, shard_id, key_commitment).await?
        {
            self.metrics.0.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(CacheObject {
                reader: verify_plaintext_reader(reader, meta, shard_id),
                plaintext: true,
            });
        }

        let parent = final_path.parent().expect("cache entry has parent");
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| io_error("create cache directory", parent, source))?;
        let download_lock = CacheDownloadLock::acquire(&self.root, shard_id).await?;
        if let Some((reader, meta)) =
            open_plaintext_hit(&final_path, &meta_path, shard_id, key_commitment).await?
        {
            self.metrics.0.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(CacheObject {
                reader: verify_plaintext_reader(reader, meta, shard_id),
                plaintext: true,
            });
        }
        remove_plaintext_entry(&final_path, &meta_path).await?;
        self.metrics.0.misses.fetch_add(1, Ordering::Relaxed);

        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary =
            final_path.with_extension(format!("plain.spk.tmp-{}-{sequence}", std::process::id()));
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|source| io_error("create plaintext temporary entry", &temporary, source))?;
        let mut remote = store
            .open_reader(object_key)
            .await
            .map_err(|source| Error::Storage { source })?;
        let cache_root = self.root.clone();
        let max_bytes = self.max_bytes;
        let metrics = self.metrics.clone();
        let expected_remote_id = *shard_id.as_bytes();
        let stream = try_stream! {
            let _download_lock = download_lock;
            let mut guard = TemporaryGuard(Some(temporary.clone()));
            let mut file = file;
            let mut remote_hasher = blake3::Hasher::new();
            let mut plaintext_hasher = blake3::Hasher::new();
            let mut remote_bytes = 0_u64;
            let mut plaintext_bytes = 0_u64;

            let mut shard_header = [0_u8; shardpack_format::SHARD_HEADER_SIZE];
            remote.read_exact(&mut shard_header).await
                .map_err(|source| io_error("read encrypted shard header", &temporary, source))?;
            account_remote(&metrics, &mut remote_hasher, &mut remote_bytes, &shard_header);
            let plain_header = shardpack_format::plaintext_shard_header(&shard_header)
                .map_err(|source| Error::Format { source })?;
            write_plain_chunk(&mut file, &temporary, &mut plaintext_hasher, &mut plaintext_bytes, &plain_header).await?;
            yield Bytes::copy_from_slice(&plain_header);

            loop {
                let mut raw_header = [0_u8; shardpack_format::BLOCK_HEADER_SIZE];
                let read = remote.read(&mut raw_header[..1]).await
                    .map_err(|source| io_error("read encrypted block header", &temporary, source))?;
                if read == 0 {
                    break;
                }
                remote.read_exact(&mut raw_header[1..]).await
                    .map_err(|source| io_error("read encrypted block header", &temporary, source))?;
                account_remote(&metrics, &mut remote_hasher, &mut remote_bytes, &raw_header);
                let header = shardpack_format::BlockHeader::decode(&raw_header)
                    .map_err(|source| Error::Format { source })?;
                let body_len = usize::try_from(header.stored_body_bytes)
                    .map_err(|_| Error::InvalidPlaintextMetadata { shard_id })?;
                let mut body = vec![0_u8; body_len];
                remote.read_exact(&mut body).await
                    .map_err(|source| io_error("read encrypted block body", &temporary, source))?;
                account_remote(&metrics, &mut remote_hasher, &mut remote_bytes, &body);
                let (plain_block_header, plain_body) =
                    shardpack_format::plaintext_block(&raw_header, body, &key)
                        .map_err(|source| Error::Format { source })?;
                write_plain_chunk(&mut file, &temporary, &mut plaintext_hasher, &mut plaintext_bytes, &plain_block_header).await?;
                yield Bytes::copy_from_slice(&plain_block_header);
                write_plain_chunk(&mut file, &temporary, &mut plaintext_hasher, &mut plaintext_bytes, &plain_body).await?;
                yield Bytes::from(plain_body);
            }

            file.sync_all().await
                .map_err(|source| io_error("sync plaintext temporary entry", &temporary, source))?;
            drop(file);
            if remote_bytes != expected_ciphertext_bytes
                || remote_hasher.finalize().as_bytes() != &expected_remote_id
            {
                Err(Error::IdentityMismatch { shard_id })?;
            }
            let plaintext_id = *plaintext_hasher.finalize().as_bytes();
            let metadata = PlaintextMetadata {
                source_id: expected_remote_id,
                plaintext_id,
                key_commitment,
                plaintext_bytes,
            };
            let _lock = CacheMutationLock::acquire(&cache_root).await?;
            publish_file(&temporary, &final_path, shard_id).await?;
            publish_plaintext_metadata(&meta_path, metadata).await?;
            guard.0 = None;
            if let Some(limit) = max_bytes {
                evict_to_limit(&cache_root, limit, &final_path, &metrics).await?;
            }
        };
        // The final plaintext identity is not known until this cold stream has
        // been completely consumed. Runtime validation uses block AEAD during
        // the first pass; subsequent hits use the published local identity.
        Ok(CacheObject {
            reader: Box::pin(StreamReader::new(
                stream.map_err(|error: Error| std::io::Error::other(error)),
            )),
            plaintext: true,
        })
    }

    /// Removes a cache entry that failed validation in the consuming reader.
    /// Already-open descriptors remain valid; a subsequent open becomes a miss
    /// and re-fetches the immutable shard.
    pub async fn invalidate(&self, shard_id: ShardId) -> Result<bool> {
        let path = self.path_for(shard_id);
        let _download_lock = CacheDownloadLock::acquire(&self.root, shard_id).await?;
        if self.mode == CacheMode::Plaintext {
            let metadata = plaintext_meta_path(&path);
            let existed = tokio::fs::try_exists(&path)
                .await
                .map_err(|source| io_error("stat plaintext cache entry", &path, source))?
                || tokio::fs::try_exists(&metadata).await.map_err(|source| {
                    io_error("stat plaintext cache metadata", &metadata, source)
                })?;
            if existed {
                let _lock = CacheMutationLock::acquire(&self.root).await?;
                remove_plaintext_entry(&path, &metadata).await?;
                self.metrics
                    .0
                    .corrupt_entries
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Ok(existed);
        }
        // A different worker may have replaced the corrupt inode after this
        // caller opened its reader. Never delete that newly repaired copy.
        if current_file_has_identity(&path, shard_id).await? == Some(true) {
            return Ok(false);
        }
        let _lock = CacheMutationLock::acquire(&self.root).await?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                self.metrics
                    .0
                    .corrupt_entries
                    .fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(source) => Err(io_error("invalidate cache entry", &path, source)),
        }
    }

    #[must_use]
    pub fn path_for(&self, shard_id: ShardId) -> PathBuf {
        let hash = shard_id.to_string();
        let root = if self.mode == CacheMode::Plaintext {
            self.root.join("plaintext")
        } else {
            self.root.clone()
        };
        root.join(&hash[..2])
            .join(&hash[2..4])
            .join(format!("{hash}.spk"))
    }

    /// Removes abandoned temporary downloads older than `minimum_age`.
    /// Published `.spk` entries are never touched.
    pub async fn cleanup_stale_partials(&self, minimum_age: std::time::Duration) -> Result<u64> {
        tokio::fs::create_dir_all(&self.root)
            .await
            .map_err(|source| io_error("create cache directory", &self.root, source))?;
        let _lock = CacheMutationLock::acquire(&self.root).await?;
        let cutoff = std::time::SystemTime::now()
            .checked_sub(minimum_age)
            .unwrap_or(std::time::UNIX_EPOCH);
        let mut directories = vec![self.root.clone()];
        let mut removed = 0_u64;
        while let Some(directory) = directories.pop() {
            let mut listing = match tokio::fs::read_dir(&directory).await {
                Ok(listing) => listing,
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => return Err(io_error("list cache directory", &directory, source)),
            };
            while let Some(entry) = listing
                .next_entry()
                .await
                .map_err(|source| io_error("list cache entry", &directory, source))?
            {
                let path = entry.path();
                if path == self.root.join(DOWNLOAD_LOCK_DIRECTORY) {
                    continue;
                }
                let metadata = entry
                    .metadata()
                    .await
                    .map_err(|source| io_error("stat cache entry", &path, source))?;
                if metadata.is_dir() {
                    directories.push(path);
                    continue;
                }
                let is_partial = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(".spk.tmp-") || name.contains(".meta.tmp-"));
                if is_partial && metadata.modified().unwrap_or(std::time::UNIX_EPOCH) <= cutoff {
                    match tokio::fs::remove_file(&path).await {
                        Ok(()) => {
                            removed += 1;
                            self.metrics
                                .0
                                .stale_partials_removed
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                        Err(source) => {
                            return Err(io_error("remove stale partial", &path, source));
                        }
                    }
                }
            }
        }
        Ok(removed)
    }
}

async fn evict_to_limit(
    root: &Path,
    max_bytes: u64,
    protected: &Path,
    metrics: &CacheMetrics,
) -> Result<()> {
    let mut directories = vec![root.to_owned()];
    let mut entries = Vec::new();
    let mut total = 0_u64;
    while let Some(directory) = directories.pop() {
        let mut listing = match tokio::fs::read_dir(&directory).await {
            Ok(listing) => listing,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(io_error("list cache directory", &directory, source)),
        };
        while let Some(entry) = listing
            .next_entry()
            .await
            .map_err(|source| io_error("list cache entry", &directory, source))?
        {
            let path = entry.path();
            if path == root.join(DOWNLOAD_LOCK_DIRECTORY) {
                continue;
            }
            let metadata = entry
                .metadata()
                .await
                .map_err(|source| io_error("stat cache entry", &path, source))?;
            if metadata.is_dir() {
                directories.push(path);
            } else if path.extension().is_some_and(|extension| extension == "spk") {
                let modified = metadata.modified().unwrap_or(std::time::UNIX_EPOCH);
                total = total.saturating_add(metadata.len());
                entries.push((modified, metadata.len(), path));
            }
        }
    }
    // The normal steady state is below the soft capacity. Avoid an O(N log N)
    // LRU sort on every completed cold miss unless eviction is actually needed.
    if total <= max_bytes {
        return Ok(());
    }
    entries.sort_unstable_by_key(|(modified, _, _)| *modified);
    for (_, bytes, path) in entries {
        if total <= max_bytes {
            break;
        }
        if path == protected {
            continue;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                let metadata = plaintext_meta_path(&path);
                let _ = tokio::fs::remove_file(metadata).await;
                total = total.saturating_sub(bytes);
                metrics.0.evicted_entries.fetch_add(1, Ordering::Relaxed);
                metrics.0.evicted_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(io_error("evict cache entry", &path, source)),
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct PlaintextMetadata {
    source_id: [u8; 32],
    plaintext_id: [u8; 32],
    key_commitment: [u8; 32],
    plaintext_bytes: u64,
}

impl PlaintextMetadata {
    fn encode(self) -> [u8; PLAINTEXT_META_SIZE] {
        let mut bytes = [0_u8; PLAINTEXT_META_SIZE];
        bytes[..8].copy_from_slice(PLAINTEXT_META_MAGIC);
        bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
        bytes[16..48].copy_from_slice(&self.source_id);
        bytes[48..80].copy_from_slice(&self.plaintext_id);
        bytes[80..112].copy_from_slice(&self.key_commitment);
        bytes[112..120].copy_from_slice(&self.plaintext_bytes.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8], shard_id: ShardId, key_commitment: [u8; 32]) -> Result<Self> {
        if bytes.len() != PLAINTEXT_META_SIZE
            || &bytes[..8] != PLAINTEXT_META_MAGIC
            || u16::from_le_bytes(bytes[8..10].try_into().expect("fixed slice")) != 1
            || bytes[10..16] != [0; 6]
        {
            return Err(Error::InvalidPlaintextMetadata { shard_id });
        }
        let source_id = bytes[16..48].try_into().expect("fixed slice");
        if source_id != *shard_id.as_bytes() {
            return Err(Error::InvalidPlaintextMetadata { shard_id });
        }
        let stored_key_commitment: [u8; 32] = bytes[80..112].try_into().expect("fixed slice");
        if stored_key_commitment != key_commitment {
            return Err(Error::PlaintextKeyMismatch { shard_id });
        }
        Ok(Self {
            source_id,
            plaintext_id: bytes[48..80].try_into().expect("fixed slice"),
            key_commitment: stored_key_commitment,
            plaintext_bytes: u64::from_le_bytes(bytes[112..120].try_into().expect("fixed slice")),
        })
    }
}

fn plaintext_meta_path(path: &Path) -> PathBuf {
    path.with_extension("spk.meta")
}

async fn open_plaintext_hit(
    path: &Path,
    meta_path: &Path,
    shard_id: ShardId,
    key_commitment: [u8; 32],
) -> Result<Option<(AsyncObjectReader, PlaintextMetadata)>> {
    let metadata_bytes = match tokio::fs::read(meta_path).await {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error("read plaintext cache metadata", meta_path, source)),
    };
    let metadata = match PlaintextMetadata::decode(&metadata_bytes, shard_id, key_commitment) {
        Ok(metadata) => metadata,
        Err(Error::InvalidPlaintextMetadata { .. }) => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(reader) = open_file_if_expected_size(path, metadata.plaintext_bytes).await? else {
        return Ok(None);
    };
    Ok(Some((reader, metadata)))
}

fn verify_plaintext_reader(
    mut reader: AsyncObjectReader,
    metadata: PlaintextMetadata,
    shard_id: ShardId,
) -> AsyncObjectReader {
    let stream = try_stream! {
        let mut hasher = blake3::Hasher::new();
        let mut byte_size = 0_u64;
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let read = reader.read(&mut buffer).await
                .map_err(|source| Error::Io {
                    operation: "read plaintext cache entry",
                    path: shard_id.to_string(),
                    source,
                })?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            byte_size = byte_size.saturating_add(read as u64);
            yield Bytes::copy_from_slice(&buffer[..read]);
        }
        if byte_size != metadata.plaintext_bytes
            || hasher.finalize().as_bytes() != &metadata.plaintext_id
        {
            Err(Error::IdentityMismatch { shard_id })?;
        }
    };
    Box::pin(StreamReader::new(
        stream.map_err(|error: Error| std::io::Error::other(error)),
    ))
}

fn account_remote(
    metrics: &CacheMetrics,
    hasher: &mut blake3::Hasher,
    bytes_read: &mut u64,
    bytes: &[u8],
) {
    hasher.update(bytes);
    *bytes_read = bytes_read.saturating_add(bytes.len() as u64);
    metrics
        .0
        .downloaded_bytes
        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
}

async fn write_plain_chunk(
    file: &mut tokio::fs::File,
    path: &Path,
    hasher: &mut blake3::Hasher,
    byte_size: &mut u64,
    bytes: &[u8],
) -> Result<()> {
    file.write_all(bytes)
        .await
        .map_err(|source| io_error("write plaintext temporary entry", path, source))?;
    hasher.update(bytes);
    *byte_size = byte_size.saturating_add(bytes.len() as u64);
    Ok(())
}

async fn publish_file(temporary: &Path, final_path: &Path, shard_id: ShardId) -> Result<()> {
    match tokio::fs::hard_link(temporary, final_path).await {
        Ok(()) => {}
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(Error::InvalidPlaintextMetadata { shard_id });
        }
        Err(source) => {
            return Err(io_error(
                "publish plaintext cache entry",
                final_path,
                source,
            ));
        }
    }
    tokio::fs::remove_file(temporary)
        .await
        .map_err(|source| io_error("remove plaintext temporary entry", temporary, source))
}

async fn publish_plaintext_metadata(path: &Path, metadata: PlaintextMetadata) -> Result<()> {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = path.with_extension(format!("meta.tmp-{}-{sequence}", std::process::id()));
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await
        .map_err(|source| io_error("create plaintext cache metadata", &temporary, source))?;
    file.write_all(&metadata.encode())
        .await
        .map_err(|source| io_error("write plaintext cache metadata", &temporary, source))?;
    file.sync_all()
        .await
        .map_err(|source| io_error("sync plaintext cache metadata", &temporary, source))?;
    drop(file);
    tokio::fs::hard_link(&temporary, path)
        .await
        .map_err(|source| io_error("publish plaintext cache metadata", path, source))?;
    tokio::fs::remove_file(&temporary)
        .await
        .map_err(|source| io_error("remove plaintext cache metadata", &temporary, source))
}

async fn remove_plaintext_entry(path: &Path, meta_path: &Path) -> Result<()> {
    for target in [path, meta_path] {
        match tokio::fs::remove_file(target).await {
            Ok(()) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(io_error("remove plaintext cache entry", target, source)),
        }
    }
    Ok(())
}

async fn verify_file(path: &Path, shard_id: ShardId, expected_bytes: u64) -> Result<bool> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|source| io_error("open cache entry", path, source))?;
    let mut hasher = blake3::Hasher::new();
    let mut byte_size = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|source| io_error("verify cache entry", path, source))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        byte_size = byte_size.saturating_add(read as u64);
    }
    Ok(byte_size == expected_bytes && hasher.finalize().as_bytes() == shard_id.as_bytes())
}

async fn current_file_has_identity(path: &Path, shard_id: ShardId) -> Result<Option<bool>> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error("open cache entry for invalidation", path, source)),
    };
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|source| io_error("verify current cache entry", path, source))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(Some(hasher.finalize().as_bytes() == shard_id.as_bytes()))
}

async fn open_file_if_expected_size(
    path: &Path,
    expected_bytes: u64,
) -> Result<Option<AsyncObjectReader>> {
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error("open cache entry", path, source)),
    };
    let byte_size = file
        .metadata()
        .await
        .map_err(|source| io_error("stat cache entry", path, source))?
        .len();
    if byte_size != expected_bytes {
        return Ok(None);
    }
    // One metadata update per shard hit keeps capacity eviction genuinely
    // recency-based without adding work to the per-block or per-record path.
    // Treat it as a cache hint: a read-only or unusual filesystem must not make
    // an otherwise valid cache entry unusable.
    let standard = file.into_std().await;
    let _ =
        standard.set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::now()));
    let file = tokio::fs::File::from_std(standard);
    Ok(Some(Box::pin(file)))
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        operation,
        path: path.display().to_string(),
        source,
    }
}

struct TemporaryGuard(Option<PathBuf>);
impl Drop for TemporaryGuard {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

struct CacheMutationLock(std::fs::File);

impl CacheMutationLock {
    async fn acquire(root: &Path) -> Result<Self> {
        Self::acquire_path(
            root.join(".shardpack-cache.lock"),
            "acquire cache mutation lock",
        )
        .await
    }

    async fn acquire_path(path: PathBuf, operation: &'static str) -> Result<Self> {
        let display = path.display().to_string();
        let file = tokio::task::spawn_blocking(move || {
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)
        })
        .await
        .map_err(|source| Error::Io {
            operation: "join cache lock task",
            path: display.clone(),
            source: std::io::Error::other(source),
        })?
        .map_err(|source| Error::Io {
            operation,
            path: display.clone(),
            source,
        })?;
        loop {
            match file.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(std::fs::TryLockError::Error(source)) => {
                    return Err(Error::Io {
                        operation,
                        path: display,
                        source,
                    });
                }
            }
        }
        Ok(Self(file))
    }
}

struct CacheDownloadLock {
    _lock: CacheMutationLock,
}

impl CacheDownloadLock {
    async fn acquire(root: &Path, shard_id: ShardId) -> Result<Self> {
        let directory = root.join(DOWNLOAD_LOCK_DIRECTORY);
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|source| io_error("create download lock directory", &directory, source))?;
        let hash = shard_id.to_string();
        let lock_path = directory.join(format!("{}.lock", &hash[..4]));
        CacheMutationLock::acquire_path(lock_path, "acquire cache download lock")
            .await
            .map(|lock| Self { _lock: lock })
    }
}

impl Drop for CacheMutationLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::stream;
    use shardpack_format::{AsyncShardReader, EncryptionKey, ShardWriter, WriterOptions};
    use tokio::io::AsyncReadExt;

    use super::{DOWNLOAD_LOCK_DIRECTORY, LocalCache, plaintext_meta_path};

    async fn publish_remote(
        store: &shardpack_storage::Store,
        key: &'static str,
        sample: u8,
        payload_bytes: usize,
    ) -> (Vec<u8>, shardpack_format::ShardSummary) {
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer
            .add_record([sample; 16], 0, &vec![sample; payload_bytes])
            .unwrap();
        let (bytes, summary) = writer.finish().unwrap();
        store
            .write_immutable(
                key,
                Box::pin(stream::once({
                    let bytes = bytes.clone();
                    async move { Ok(Bytes::from(bytes)) }
                })),
            )
            .await
            .unwrap();
        (bytes, summary)
    }

    #[tokio::test]
    async fn tees_miss_then_survives_remote_loss() {
        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([1; 16], 0, b"payload").unwrap();
        let (bytes, summary) = writer.finish().unwrap();
        let key = "shards/object.spk";
        store
            .write_immutable(
                key,
                Box::pin(stream::once({
                    let bytes = bytes.clone();
                    async move { Ok(Bytes::from(bytes)) }
                })),
            )
            .await
            .unwrap();
        let cache = LocalCache::new(cache_root.path());
        let id = shardpack_manifest::ShardId(summary.shard_id);
        let mut miss = cache
            .open(&store, key, id, summary.byte_size)
            .await
            .unwrap();
        let mut first = Vec::new();
        miss.read_to_end(&mut first).await.unwrap();
        assert_eq!(first, bytes);
        store.delete(key).await.unwrap();
        let mut hit = cache
            .open(&store, key, id, summary.byte_size)
            .await
            .unwrap();
        let mut second = Vec::new();
        hit.read_to_end(&mut second).await.unwrap();
        assert_eq!(second, bytes);
        let metrics = cache.metrics().snapshot();
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.hits, 1);
        assert_eq!(metrics.downloaded_bytes, bytes.len() as u64);
    }

    #[tokio::test]
    async fn plaintext_cache_trains_during_fill_then_reads_without_decryption() {
        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let key = EncryptionKey::new([0x5a; 32]);
        let mut writer =
            ShardWriter::new_encrypted(Vec::new(), WriterOptions::default(), key.clone()).unwrap();
        writer.add_record([1; 16], 7, b"first").unwrap();
        writer.add_record([2; 16], 9, b"second").unwrap();
        let (ciphertext, summary) = writer.finish().unwrap();
        let object_key = "shards/encrypted.spk";
        store
            .write_bytes_immutable(object_key, Bytes::from(ciphertext))
            .await
            .unwrap();

        let id = shardpack_manifest::ShardId(summary.shard_id);
        let cache = LocalCache::new(cache_root.path()).with_plaintext(key);
        let cold = cache
            .open_object(&store, object_key, id, summary.byte_size)
            .await
            .unwrap();
        assert!(cold.plaintext);
        let mut first_pass = AsyncShardReader::new(cold.reader).await.unwrap();
        assert_eq!(
            first_pass.next_record().await.unwrap().unwrap().payload,
            b"first"[..]
        );
        assert_eq!(
            first_pass.next_record().await.unwrap().unwrap().payload,
            b"second"[..]
        );
        assert!(first_pass.next_record().await.unwrap().is_none());
        assert!(cache.path_for(id).is_file());

        let wrong_key_cache =
            LocalCache::new(cache_root.path()).with_plaintext(EncryptionKey::new([0x99; 32]));
        assert!(matches!(
            wrong_key_cache
                .open_object(&store, object_key, id, summary.byte_size)
                .await,
            Err(super::Error::PlaintextKeyMismatch { .. })
        ));

        store.delete(object_key).await.unwrap();
        let hot = cache
            .open_object(&store, object_key, id, summary.byte_size)
            .await
            .unwrap();
        assert!(hot.plaintext);
        let mut second_pass = AsyncShardReader::new(hot.reader).await.unwrap();
        assert_eq!(
            second_pass.next_record().await.unwrap().unwrap().payload,
            b"first"[..]
        );
        assert_eq!(
            second_pass.next_record().await.unwrap().unwrap().payload,
            b"second"[..]
        );
        assert!(second_pass.next_record().await.unwrap().is_none());
        let metrics = cache.metrics().snapshot();
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.hits, 1);
        assert_eq!(metrics.downloaded_bytes, summary.byte_size);
    }

    #[tokio::test]
    async fn plaintext_cache_never_publishes_with_the_wrong_key() {
        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let mut writer = ShardWriter::new_encrypted(
            Vec::new(),
            WriterOptions::default(),
            EncryptionKey::new([1; 32]),
        )
        .unwrap();
        writer.add_record([1; 16], 0, b"secret").unwrap();
        let (ciphertext, summary) = writer.finish().unwrap();
        let object_key = "shards/wrong-key.spk";
        store
            .write_bytes_immutable(object_key, Bytes::from(ciphertext))
            .await
            .unwrap();
        let id = shardpack_manifest::ShardId(summary.shard_id);
        let cache = LocalCache::new(cache_root.path()).with_plaintext(EncryptionKey::new([2; 32]));
        let mut reader = cache
            .open(&store, object_key, id, summary.byte_size)
            .await
            .unwrap();
        assert!(reader.read_to_end(&mut Vec::new()).await.is_err());
        assert!(!cache.path_for(id).exists());
        assert!(!plaintext_meta_path(&cache.path_for(id)).exists());
    }

    #[tokio::test]
    async fn concurrent_misses_publish_one_verified_entry() {
        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([9; 16], 0, &vec![7; 256 * 1024]).unwrap();
        let (bytes, summary) = writer.finish().unwrap();
        let key = "shards/concurrent.spk";
        store
            .write_immutable(
                key,
                Box::pin(stream::once({
                    let bytes = bytes.clone();
                    async move { Ok(Bytes::from(bytes)) }
                })),
            )
            .await
            .unwrap();
        let cache = LocalCache::new(cache_root.path());
        let id = shardpack_manifest::ShardId(summary.shard_id);
        let byte_size = summary.byte_size;
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                let mut reader = cache.open(&store, key, id, byte_size).await.unwrap();
                let mut output = Vec::new();
                reader.read_to_end(&mut output).await.unwrap();
                output
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap(), bytes);
        }
        assert!(cache.path_for(id).is_file());
        let metrics = cache.metrics().snapshot();
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.hits, 7);
        assert_eq!(metrics.downloaded_bytes, byte_size);
        let lock_directory = cache_root.path().join(DOWNLOAD_LOCK_DIRECTORY);
        let mut locks = tokio::fs::read_dir(lock_directory).await.unwrap();
        assert!(locks.next_entry().await.unwrap().is_some());
        assert!(locks.next_entry().await.unwrap().is_none());
        assert!(
            cache
                .cleanup_stale_partials(std::time::Duration::ZERO)
                .await
                .unwrap()
                == 0
        );
    }

    #[tokio::test]
    async fn hit_is_opened_without_an_eager_full_file_digest() {
        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([4; 16], 0, b"payload").unwrap();
        let (bytes, summary) = writer.finish().unwrap();
        let key = "shards/lazy-validation.spk";
        store
            .write_immutable(
                key,
                Box::pin(stream::once({
                    let bytes = bytes.clone();
                    async move { Ok(Bytes::from(bytes)) }
                })),
            )
            .await
            .unwrap();
        let cache = LocalCache::new(cache_root.path());
        let id = shardpack_manifest::ShardId(summary.shard_id);
        let mut populate = cache
            .open(&store, key, id, summary.byte_size)
            .await
            .unwrap();
        tokio::io::copy(&mut populate, &mut tokio::io::sink())
            .await
            .unwrap();

        let mut corrupt = bytes;
        *corrupt.last_mut().unwrap() ^= 0x80;
        tokio::fs::write(cache.path_for(id), &corrupt)
            .await
            .unwrap();
        store.delete(key).await.unwrap();

        let mut hit = cache
            .open(&store, key, id, summary.byte_size)
            .await
            .unwrap();
        let mut observed = Vec::new();
        hit.read_to_end(&mut observed).await.unwrap();
        assert_eq!(observed, corrupt);
        assert_eq!(cache.metrics().snapshot().hits, 1);
        assert!(cache.invalidate(id).await.unwrap());
        assert_eq!(cache.metrics().snapshot().corrupt_entries, 1);
    }

    #[tokio::test]
    async fn stale_invalidation_does_not_delete_a_repaired_entry() {
        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let (bytes, summary) = publish_remote(&store, "shards/repaired.spk", 8, 4096).await;
        let id = shardpack_manifest::ShardId(summary.shard_id);
        let cache = LocalCache::new(cache_root.path());
        let mut populate = cache
            .open(&store, "shards/repaired.spk", id, summary.byte_size)
            .await
            .unwrap();
        tokio::io::copy(&mut populate, &mut tokio::io::sink())
            .await
            .unwrap();

        assert!(!cache.invalidate(id).await.unwrap());
        assert_eq!(tokio::fs::read(cache.path_for(id)).await.unwrap(), bytes);
        assert_eq!(cache.metrics().snapshot().corrupt_entries, 0);
    }

    #[tokio::test]
    async fn cleans_only_abandoned_partial_downloads() {
        let root = tempfile::tempdir().unwrap();
        let cache = LocalCache::new(root.path());
        let directory = root.path().join("aa/bb");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        tokio::fs::write(directory.join("value.spk.tmp-dead"), b"partial")
            .await
            .unwrap();
        tokio::fs::write(directory.join("value.spk"), b"published")
            .await
            .unwrap();
        assert_eq!(
            cache
                .cleanup_stale_partials(std::time::Duration::ZERO)
                .await
                .unwrap(),
            1
        );
        assert!(directory.join("value.spk").is_file());
        assert_eq!(cache.metrics().snapshot().stale_partials_removed, 1);
    }

    #[tokio::test]
    async fn eviction_does_not_invalidate_an_already_open_reader() {
        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let (first_bytes, first_summary) =
            publish_remote(&store, "shards/evict-first.spk", 1, 1024).await;
        let (second_bytes, second_summary) =
            publish_remote(&store, "shards/evict-second.spk", 2, 1024).await;
        let cache = LocalCache::new(cache_root.path()).with_max_bytes(second_summary.byte_size);
        let first_id = shardpack_manifest::ShardId(first_summary.shard_id);
        let second_id = shardpack_manifest::ShardId(second_summary.shard_id);

        let mut populate_first = cache
            .open(
                &store,
                "shards/evict-first.spk",
                first_id,
                first_summary.byte_size,
            )
            .await
            .unwrap();
        tokio::io::copy(&mut populate_first, &mut tokio::io::sink())
            .await
            .unwrap();
        let mut active_reader = cache
            .open(
                &store,
                "shards/evict-first.spk",
                first_id,
                first_summary.byte_size,
            )
            .await
            .unwrap();

        let mut populate_second = cache
            .open(
                &store,
                "shards/evict-second.spk",
                second_id,
                second_summary.byte_size,
            )
            .await
            .unwrap();
        let mut observed_second = Vec::new();
        populate_second
            .read_to_end(&mut observed_second)
            .await
            .unwrap();
        assert_eq!(observed_second, second_bytes);
        assert!(!cache.path_for(first_id).exists());
        assert!(cache.path_for(second_id).exists());

        let mut observed_first = Vec::new();
        active_reader
            .read_to_end(&mut observed_first)
            .await
            .unwrap();
        assert_eq!(observed_first, first_bytes);
        let metrics = cache.metrics().snapshot();
        assert_eq!(metrics.evicted_entries, 1);
        assert_eq!(metrics.evicted_bytes, first_summary.byte_size);
    }

    #[tokio::test]
    async fn oversized_protected_entry_may_exceed_soft_capacity() {
        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let (bytes, summary) = publish_remote(&store, "shards/oversized.spk", 3, 4096).await;
        let id = shardpack_manifest::ShardId(summary.shard_id);
        let cache = LocalCache::new(cache_root.path()).with_max_bytes(1);
        let mut reader = cache
            .open(&store, "shards/oversized.spk", id, summary.byte_size)
            .await
            .unwrap();
        let mut observed = Vec::new();
        reader.read_to_end(&mut observed).await.unwrap();
        assert_eq!(observed, bytes);
        assert_eq!(
            tokio::fs::metadata(cache.path_for(id)).await.unwrap().len(),
            summary.byte_size
        );
        assert_eq!(cache.metrics().snapshot().evicted_entries, 0);
    }

    #[tokio::test]
    async fn eviction_retains_the_most_recently_hit_entry() {
        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let (_, first) = publish_remote(&store, "shards/lru-first.spk", 1, 1024).await;
        let (_, second) = publish_remote(&store, "shards/lru-second.spk", 2, 1024).await;
        let (_, third) = publish_remote(&store, "shards/lru-third.spk", 3, 1024).await;
        assert_eq!(first.byte_size, second.byte_size);
        assert_eq!(second.byte_size, third.byte_size);
        let first_id = shardpack_manifest::ShardId(first.shard_id);
        let second_id = shardpack_manifest::ShardId(second.shard_id);
        let third_id = shardpack_manifest::ShardId(third.shard_id);
        let cache = LocalCache::new(cache_root.path()).with_max_bytes(first.byte_size * 2);

        for (key, id, bytes) in [
            ("shards/lru-first.spk", first_id, first.byte_size),
            ("shards/lru-second.spk", second_id, second.byte_size),
        ] {
            let mut reader = cache.open(&store, key, id, bytes).await.unwrap();
            tokio::io::copy(&mut reader, &mut tokio::io::sink())
                .await
                .unwrap();
        }

        let old = std::time::UNIX_EPOCH + std::time::Duration::from_secs(10);
        std::fs::File::open(cache.path_for(first_id))
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(old))
            .unwrap();
        std::fs::File::open(cache.path_for(second_id))
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(old))
            .unwrap();

        // Refresh the first entry after both candidates were made equally old.
        drop(
            cache
                .open(&store, "shards/lru-first.spk", first_id, first.byte_size)
                .await
                .unwrap(),
        );
        let mut third_reader = cache
            .open(&store, "shards/lru-third.spk", third_id, third.byte_size)
            .await
            .unwrap();
        tokio::io::copy(&mut third_reader, &mut tokio::io::sink())
            .await
            .unwrap();

        assert!(cache.path_for(first_id).is_file());
        assert!(!cache.path_for(second_id).exists());
        assert!(cache.path_for(third_id).is_file());
    }

    #[tokio::test]
    async fn identity_mismatch_never_publishes_or_leaves_a_partial() {
        let remote_root = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(remote_root.path()).unwrap();
        let (_bytes, summary) = publish_remote(&store, "shards/mismatch.spk", 4, 128).await;
        let wrong_id = shardpack_manifest::ShardId([0xff; 32]);
        let cache = LocalCache::new(cache_root.path());
        let mut reader = cache
            .open(&store, "shards/mismatch.spk", wrong_id, summary.byte_size)
            .await
            .unwrap();
        assert!(reader.read_to_end(&mut Vec::new()).await.is_err());
        drop(reader);

        assert!(!cache.path_for(wrong_id).exists());
        assert_eq!(
            cache
                .cleanup_stale_partials(std::time::Duration::ZERO)
                .await
                .unwrap(),
            0
        );
    }
}
