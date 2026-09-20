use std::{io::Write, path::Path};

use bytes::Bytes;
use futures_util::{TryStreamExt, stream};
use prost::Message;
use shardpack_format::{
    BlockCodec, ContentFingerprint, EncryptionKey, ShardSummary, ShardWriter, WriterOptions,
};
use shardpack_manifest::{layout_id, proto};
use shardpack_storage::{Error as StorageError, Store};

use crate::shuffle::ShuffledSource;
use crate::{EncodedRecord, Error, Result, SourceReader};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OfflineShuffleOptions {
    pub seed: u64,
    pub max_records: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MissingDataPolicy {
    #[default]
    Fail,
    /// The source adapter intentionally omitted this many records. Adapters
    /// must report the count explicitly; arbitrary source errors still fail.
    Skip { skipped_records: u64 },
}

#[derive(Clone, Debug)]
pub struct MaterializeEncryption {
    pub key_id: String,
    pub key: EncryptionKey,
}

#[derive(Clone, Debug)]
pub struct MaterializeOptions {
    pub target_shard_bytes: u64,
    pub target_block_bytes: usize,
    pub block_codec: BlockCodec,
    pub zstd_level: i32,
    pub object_prefix: String,
    pub layout_prefix: String,
    pub builder_version: String,
    pub portability: proto::Portability,
    pub durability: proto::Durability,
    pub offline_shuffle: Option<OfflineShuffleOptions>,
    pub missing_data_policy: MissingDataPolicy,
    pub encryption: Option<MaterializeEncryption>,
}

impl Default for MaterializeOptions {
    fn default() -> Self {
        Self {
            target_shard_bytes: 1024 * 1024 * 1024,
            target_block_bytes: 16 * 1024 * 1024,
            block_codec: BlockCodec::None,
            zstd_level: 1,
            object_prefix: "shards".into(),
            layout_prefix: "layouts".into(),
            builder_version: env!("CARGO_PKG_VERSION").into(),
            portability: proto::Portability::SelfContained,
            durability: proto::Durability::Durable,
            offline_shuffle: None,
            missing_data_policy: MissingDataPolicy::Fail,
            encryption: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Materialization {
    pub layout: proto::LayoutManifest,
    pub layout_object_key: String,
}

pub async fn materialize_layout<S: SourceReader>(
    source: &mut S,
    snapshot_id: [u8; 32],
    store: &Store,
    staging_directory: &Path,
    options: &MaterializeOptions,
) -> Result<Materialization> {
    materialize_layout_inner(
        source,
        snapshot_id,
        store,
        staging_directory,
        options,
        None,
        None,
    )
    .await
}

/// Coordinates a full materialization with GC and control-plane mutations.
pub async fn materialize_layout_coordinated<S: SourceReader>(
    source: &mut S,
    snapshot_id: [u8; 32],
    store: &Store,
    catalog: &shardpack_catalog::Catalog,
    staging_directory: &Path,
    options: &MaterializeOptions,
) -> Result<Materialization> {
    let lease = catalog
        .acquire_barrier(shardpack_catalog::new_owner_id(), "materialization")
        .await
        .map_err(|source| Error::Catalog { source })?;
    let operation =
        materialize_layout(source, snapshot_id, store, staging_directory, options).await;
    finish_coordinated(catalog, &lease, operation).await
}

pub(crate) async fn finish_coordinated<T>(
    catalog: &shardpack_catalog::Catalog,
    lease: &shardpack_catalog::BarrierLease,
    operation: Result<T>,
) -> Result<T> {
    let release = catalog
        .release_barrier(lease)
        .await
        .map_err(|source| Error::Catalog { source });
    match operation {
        Err(error) => {
            let _ = release;
            Err(error)
        }
        Ok(value) => {
            release?;
            Ok(value)
        }
    }
}

async fn materialize_layout_inner<S: SourceReader>(
    source: &mut S,
    snapshot_id: [u8; 32],
    store: &Store,
    staging_directory: &Path,
    options: &MaterializeOptions,
    expected_records: Option<u64>,
    split_name: Option<&str>,
) -> Result<Materialization> {
    validate_options(options)?;
    let mut source = ShuffledSource::new(source, options.offline_shuffle)?;
    let mut pending = source.next_record()?;
    let mut shards = Vec::new();
    let mut total_fingerprint = ContentFingerprint::default();
    let mut total_bytes = 0_u64;

    while pending.is_some() {
        let mut staging = tempfile::NamedTempFile::new_in(staging_directory)
            .map_err(|source| Error::Staging { source })?;
        let summary = {
            let writer_options = WriterOptions {
                target_block_bytes: options.target_block_bytes,
                codec: options.block_codec,
                zstd_level: options.zstd_level,
            };
            let mut writer = if let Some(encryption) = &options.encryption {
                ShardWriter::new_encrypted(
                    staging.as_file_mut(),
                    writer_options,
                    encryption.key.clone(),
                )
            } else {
                ShardWriter::new(staging.as_file_mut(), writer_options)
            }
            .map_err(|source| Error::Format { source })?;
            let mut logical_bytes = 0_u64;
            let mut record_count = 0_u64;
            loop {
                let Some(record) = pending.take() else { break };
                let record_bytes = record.payload.len() as u64;
                if record_count > 0
                    && logical_bytes.saturating_add(record_bytes) > options.target_shard_bytes
                {
                    pending = Some(record);
                    break;
                }
                write_record(&mut writer, &record)?;
                logical_bytes = logical_bytes.saturating_add(record_bytes);
                record_count += 1;
                pending = source.next_record()?;
            }
            let (_, summary) = writer.finish().map_err(|source| Error::Format { source })?;
            summary
        };

        let key = shard_object_key(&options.object_prefix, &summary.shard_id);
        match store.write_file_immutable(&key, staging.path()).await {
            Ok(()) | Err(StorageError::AlreadyExists { .. }) => {}
            Err(source) => return Err(Error::Storage { source }),
        }
        verify_uploaded_shard(store, &key, &summary).await?;
        total_fingerprint.merge(&summary.content_fingerprint);
        total_bytes = total_bytes
            .checked_add(summary.byte_size)
            .expect("layout size overflow");
        shards.push(shard_meta(&key, &summary));
    }

    let (state, materialization_stats) = completion_fields(
        options.missing_data_policy,
        expected_records,
        total_fingerprint.record_count,
    )?;
    let mut layout = proto::LayoutManifest {
        snapshot_id: snapshot_id.to_vec(),
        builder_version: options.builder_version.clone(),
        recipe: Some(proto::LayoutRecipe {
            target_shard_bytes: options.target_shard_bytes,
            target_block_bytes: options.target_block_bytes as u64,
            block_codec: match options.block_codec {
                BlockCodec::None => "none",
                BlockCodec::Zstd => "zstd",
            }
            .into(),
            shuffle_seed: options.offline_shuffle.map(|shuffle| shuffle.seed),
        }),
        portability: options.portability as i32,
        durability: options.durability as i32,
        total_records: total_fingerprint.record_count,
        total_bytes,
        content_fingerprint: Some(proto_fingerprint(total_fingerprint)),
        shards,
        state,
        materialization_stats,
        split_name: split_name.map(str::to_owned),
        created_at: Some(now_timestamp()),
        encryption: options
            .encryption
            .as_ref()
            .map(|encryption| proto::EncryptionDescriptor {
                algorithm: proto::EncryptionAlgorithm::Aes256Gcm as i32,
                key_id: encryption.key_id.clone(),
            }),
        ..Default::default()
    };
    layout.layout_id = layout_id(&layout).0.to_vec();
    shardpack_manifest::validate_layout(&layout).map_err(|source| Error::Manifest { source })?;
    let layout_id: [u8; 32] = layout
        .layout_id
        .as_slice()
        .try_into()
        .expect("validated layout ID");
    let layout_object_key = manifest_object_key(&options.layout_prefix, &layout_id);
    layout = publish_layout(store, &layout_object_key, layout).await?;
    Ok(Materialization {
        layout,
        layout_object_key,
    })
}

pub(crate) fn completion_fields(
    policy: MissingDataPolicy,
    expected_records: Option<u64>,
    written_records: u64,
) -> Result<(i32, Option<proto::MaterializationStats>)> {
    match policy {
        MissingDataPolicy::Fail => {
            if expected_records.is_some_and(|expected| expected != written_records) {
                return Err(Error::SnapshotLayoutMismatch {
                    reason: "materialized record count differs from snapshot",
                });
            }
            Ok((proto::LayoutState::Complete as i32, None))
        }
        MissingDataPolicy::Skip { skipped_records } => {
            let actual_expected =
                written_records
                    .checked_add(skipped_records)
                    .ok_or(Error::InvalidOption {
                        field: "missing_data_policy",
                        reason: "record count overflow",
                    })?;
            if expected_records.is_some_and(|expected| expected != actual_expected) {
                return Err(Error::SnapshotLayoutMismatch {
                    reason: "written plus skipped records differs from snapshot",
                });
            }
            Ok((
                if skipped_records == 0 {
                    proto::LayoutState::Complete as i32
                } else {
                    proto::LayoutState::Incomplete as i32
                },
                Some(proto::MaterializationStats {
                    expected_records: expected_records.unwrap_or(actual_expected),
                    written_records,
                    skipped_records,
                }),
            ))
        }
    }
}

pub async fn materialize_snapshot_layout<S: SourceReader>(
    source: &mut S,
    snapshot: &proto::SnapshotManifest,
    store: &Store,
    staging_directory: &Path,
    options: &MaterializeOptions,
) -> Result<Materialization> {
    shardpack_manifest::validate_snapshot(snapshot).map_err(|source| Error::Manifest { source })?;
    let snapshot_id: [u8; 32] = snapshot
        .snapshot_id
        .as_slice()
        .try_into()
        .expect("validated snapshot ID");
    let mut effective = options.clone();
    effective.portability = proto::Portability::try_from(snapshot.portability).map_err(|_| {
        Error::SnapshotLayoutMismatch {
            reason: "snapshot portability is invalid",
        }
    })?;
    materialize_layout_inner(
        source,
        snapshot_id,
        store,
        staging_directory,
        &effective,
        Some(snapshot.logical_record_count),
        None,
    )
    .await
}

pub async fn materialize_snapshot_split_layout<S: SourceReader>(
    source: &mut S,
    snapshot: &proto::SnapshotManifest,
    split_name: &str,
    store: &Store,
    staging_directory: &Path,
    options: &MaterializeOptions,
) -> Result<Materialization> {
    shardpack_manifest::validate_snapshot(snapshot).map_err(|source| Error::Manifest { source })?;
    let split = snapshot
        .splits
        .iter()
        .find(|split| split.name == split_name)
        .ok_or(Error::SnapshotLayoutMismatch {
            reason: "requested split does not exist in snapshot",
        })?;
    let snapshot_id = snapshot
        .snapshot_id
        .as_slice()
        .try_into()
        .expect("validated snapshot ID");
    let mut effective = options.clone();
    effective.portability = proto::Portability::try_from(snapshot.portability).map_err(|_| {
        Error::SnapshotLayoutMismatch {
            reason: "snapshot portability is invalid",
        }
    })?;
    materialize_layout_inner(
        source,
        snapshot_id,
        store,
        staging_directory,
        &effective,
        Some(split.record_count),
        Some(split_name),
    )
    .await
}

pub async fn materialize_snapshot_split_layout_coordinated<S: SourceReader>(
    source: &mut S,
    snapshot: &proto::SnapshotManifest,
    split_name: &str,
    store: &Store,
    catalog: &shardpack_catalog::Catalog,
    staging_directory: &Path,
    options: &MaterializeOptions,
) -> Result<Materialization> {
    let lease = catalog
        .acquire_barrier(shardpack_catalog::new_owner_id(), "split-materialization")
        .await
        .map_err(|source| Error::Catalog { source })?;
    let operation = materialize_snapshot_split_layout(
        source,
        snapshot,
        split_name,
        store,
        staging_directory,
        options,
    )
    .await;
    finish_coordinated(catalog, &lease, operation).await
}

pub async fn materialize_snapshot_layout_coordinated<S: SourceReader>(
    source: &mut S,
    snapshot: &proto::SnapshotManifest,
    store: &Store,
    catalog: &shardpack_catalog::Catalog,
    staging_directory: &Path,
    options: &MaterializeOptions,
) -> Result<Materialization> {
    let lease = catalog
        .acquire_barrier(
            shardpack_catalog::new_owner_id(),
            "snapshot-materialization",
        )
        .await
        .map_err(|source| Error::Catalog { source })?;
    let operation =
        materialize_snapshot_layout(source, snapshot, store, staging_directory, options).await;
    finish_coordinated(catalog, &lease, operation).await
}

pub(crate) fn write_record<W: Write>(
    writer: &mut ShardWriter<W>,
    record: &EncodedRecord,
) -> Result<()> {
    writer
        .add_record(record.sample_id, record.flags, &record.payload)
        .map_err(|source| Error::Format { source })
}

pub(crate) async fn verify_uploaded_shard(
    store: &Store,
    key: &str,
    summary: &ShardSummary,
) -> Result<()> {
    let mut stream = store
        .stream(key)
        .await
        .map_err(|source| Error::Storage { source })?;
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0_u64;
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|source| Error::Storage { source })?
    {
        hasher.update(&chunk);
        bytes = bytes.saturating_add(chunk.len() as u64);
    }
    if bytes != summary.byte_size || hasher.finalize().as_bytes() != &summary.shard_id {
        return Err(Error::ObjectIdentityMismatch {
            key: key.to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn shard_meta(key: &str, summary: &ShardSummary) -> proto::ShardMeta {
    proto::ShardMeta {
        shard_id: summary.shard_id.to_vec(),
        object_uri: key.to_owned(),
        byte_size: summary.byte_size,
        record_count: summary.record_count,
        block_count: summary.block_count,
        content_fingerprint: Some(proto_fingerprint(summary.content_fingerprint)),
    }
}

pub(crate) fn proto_fingerprint(value: ContentFingerprint) -> proto::ContentFingerprint {
    proto::ContentFingerprint {
        record_count: value.record_count,
        accumulator_a: value.accumulator_a.to_vec(),
        accumulator_b: value.accumulator_b.to_vec(),
    }
}

pub(crate) fn validate_options(options: &MaterializeOptions) -> Result<()> {
    if options
        .encryption
        .as_ref()
        .is_some_and(|encryption| encryption.key_id.is_empty())
    {
        return Err(Error::InvalidOption {
            field: "encryption.key_id",
            reason: "must not be empty",
        });
    }
    if options.target_shard_bytes == 0 {
        return Err(Error::InvalidOption {
            field: "target_shard_bytes",
            reason: "must be greater than zero",
        });
    }
    if options.target_block_bytes == 0 {
        return Err(Error::InvalidOption {
            field: "target_block_bytes",
            reason: "must be greater than zero",
        });
    }
    if options
        .offline_shuffle
        .is_some_and(|shuffle| shuffle.max_records == 0 || shuffle.max_bytes == 0)
    {
        return Err(Error::InvalidOption {
            field: "offline_shuffle",
            reason: "record and byte limits must be greater than zero",
        });
    }
    if options.object_prefix.is_empty()
        || options.object_prefix.starts_with('/')
        || options.object_prefix.ends_with('/')
    {
        return Err(Error::InvalidOption {
            field: "object_prefix",
            reason: "must be a normalized relative prefix",
        });
    }
    if options.layout_prefix.is_empty()
        || options.layout_prefix.starts_with('/')
        || options.layout_prefix.ends_with('/')
    {
        return Err(Error::InvalidOption {
            field: "layout_prefix",
            reason: "must be a normalized relative prefix",
        });
    }
    Ok(())
}

#[must_use]
pub fn shard_object_key(prefix: &str, shard_id: &[u8; 32]) -> String {
    let hash = hex::encode(shard_id);
    format!("{prefix}/{}/{}/{}.spk", &hash[..2], &hash[2..4], hash)
}

pub(crate) fn manifest_object_key(prefix: &str, layout_id: &[u8; 32]) -> String {
    let hash = hex::encode(layout_id);
    format!("{prefix}/{}/{}/{}.layout.pb", &hash[..2], &hash[2..4], hash)
}

pub(crate) async fn publish_layout(
    store: &Store,
    key: &str,
    layout: proto::LayoutManifest,
) -> Result<proto::LayoutManifest> {
    let manifest = layout.encode_to_vec();
    let input = stream::once({
        let manifest = manifest.clone();
        async move { Ok(Bytes::from(manifest)) }
    });
    match store.write_immutable(key, Box::pin(input)).await {
        Ok(()) => Ok(layout),
        Err(StorageError::AlreadyExists { .. }) => {
            let mut existing = store
                .stream(key)
                .await
                .map_err(|source| Error::Storage { source })?;
            let mut bytes = Vec::new();
            while let Some(chunk) = existing
                .try_next()
                .await
                .map_err(|source| Error::Storage { source })?
            {
                bytes.extend_from_slice(&chunk);
            }
            let existing = proto::LayoutManifest::decode(bytes.as_slice()).map_err(|source| {
                Error::Manifest {
                    source: shardpack_manifest::Error::Decode { source },
                }
            })?;
            shardpack_manifest::validate_layout(&existing)
                .map_err(|source| Error::Manifest { source })?;
            if existing.layout_id == layout.layout_id {
                Ok(existing)
            } else {
                Err(Error::ObjectIdentityMismatch {
                    key: key.to_owned(),
                })
            }
        }
        Err(source) => Err(Error::Storage { source }),
    }
}

pub(crate) fn now_timestamp() -> prost_types::Timestamp {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(elapsed.subsec_nanos()).expect("nanoseconds fit i32"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MaterializeEncryption, MaterializeOptions, MissingDataPolicy, materialize_layout,
        materialize_snapshot_split_layout,
    };
    use crate::{EncodedRecord, Result, SourceReader};
    use bytes::Bytes;
    use prost::Message;
    use shardpack_manifest::proto;

    struct Records(std::vec::IntoIter<EncodedRecord>);
    impl SourceReader for Records {
        fn next_record(&mut self) -> Result<Option<EncodedRecord>> {
            Ok(self.0.next())
        }
    }

    #[tokio::test]
    async fn writes_multiple_content_addressed_shards_and_layout() {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let records = (0..5_u8)
            .map(|n| EncodedRecord {
                sample_id: [n; 16],
                flags: 0,
                payload: Bytes::from(vec![n; 10]),
            })
            .collect::<Vec<_>>();
        let mut source = Records(records.into_iter());
        let options = MaterializeOptions {
            target_shard_bytes: 20,
            target_block_bytes: 16,
            ..Default::default()
        };
        let result = materialize_layout(&mut source, [9; 32], &store, staging.path(), &options)
            .await
            .unwrap();
        assert_eq!(result.layout.total_records, 5);
        assert_eq!(result.layout.shards.len(), 3);
        store
            .operator()
            .stat(&result.layout_object_key)
            .await
            .unwrap();
        for shard in &result.layout.shards {
            store.operator().stat(&shard.object_uri).await.unwrap();
        }

        let records = (0..5_u8)
            .map(|n| EncodedRecord {
                sample_id: [n; 16],
                flags: 0,
                payload: Bytes::from(vec![n; 10]),
            })
            .collect::<Vec<_>>();
        let mut retry_source = Records(records.into_iter());
        let retry =
            materialize_layout(&mut retry_source, [9; 32], &store, staging.path(), &options)
                .await
                .unwrap();
        assert_eq!(result.layout.encode_to_vec(), retry.layout.encode_to_vec());
    }

    #[tokio::test]
    async fn materializes_encrypted_shards_and_records_only_the_key_id() {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let key = shardpack_format::EncryptionKey::new([0x81; 32]);
        let mut source = Records(
            vec![EncodedRecord {
                sample_id: [8; 16],
                flags: 0,
                payload: Bytes::from_static(b"private"),
            }]
            .into_iter(),
        );
        let result = materialize_layout(
            &mut source,
            [8; 32],
            &store,
            staging.path(),
            &MaterializeOptions {
                encryption: Some(MaterializeEncryption {
                    key_id: "kms://dataset/key-8".into(),
                    key: key.clone(),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            result.layout.encryption.as_ref().unwrap().key_id,
            "kms://dataset/key-8"
        );
        let object = store
            .open_reader(&result.layout.shards[0].object_uri)
            .await
            .unwrap();
        let mut reader = shardpack_format::AsyncShardReader::new_encrypted(object, key)
            .await
            .unwrap();
        assert_eq!(
            reader.next_record().await.unwrap().unwrap().payload,
            "private"
        );
    }

    #[tokio::test]
    async fn publishes_a_layout_for_one_snapshot_split() {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let mut snapshot = proto::SnapshotManifest {
            dataset_id: "split-dataset".into(),
            source: Some(proto::SnapshotSource {
                source: Some(proto::snapshot_source::Source::Imported(
                    proto::ImportedSource { description: None },
                )),
            }),
            record_schema: Some(proto::RecordSchemaDescriptor {
                codec_name: "test".into(),
                codec_version: 1,
                schema_id: vec![1; 32],
                embedded_schema: None,
            }),
            splits: vec![
                proto::SplitDescriptor {
                    name: "train".into(),
                    record_count: 2,
                    assignment: "hash(sample_id, 7)".into(),
                    hash_upper_bound: Some(8),
                },
                proto::SplitDescriptor {
                    name: "val".into(),
                    record_count: 1,
                    assignment: "hash(sample_id, 7)".into(),
                    hash_upper_bound: Some(10),
                },
            ],
            split_assignment: Some(proto::SplitAssignment {
                seed: 7,
                modulus: 10,
                group_field: None,
            }),
            portability: proto::Portability::SelfContained as i32,
            durability: proto::Durability::Durable as i32,
            logical_record_count: 3,
            ..Default::default()
        };
        snapshot.snapshot_id = shardpack_manifest::snapshot_id(&snapshot).0.to_vec();
        let records = (0..2_u8)
            .map(|value| EncodedRecord {
                sample_id: [value; 16],
                flags: 0,
                payload: Bytes::from(vec![value; 4]),
            })
            .collect::<Vec<_>>();
        let mut source = Records(records.into_iter());
        let result = materialize_snapshot_split_layout(
            &mut source,
            &snapshot,
            "train",
            &store,
            staging.path(),
            &MaterializeOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(result.layout.split_name.as_deref(), Some("train"));
        assert_eq!(result.layout.total_records, 2);
    }

    #[tokio::test]
    async fn skip_policy_publishes_explicitly_incomplete_counts() {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let mut source = Records(
            vec![EncodedRecord {
                sample_id: [1; 16],
                flags: 0,
                payload: Bytes::from_static(b"available"),
            }]
            .into_iter(),
        );
        let result = materialize_layout(
            &mut source,
            [4; 32],
            &store,
            staging.path(),
            &MaterializeOptions {
                missing_data_policy: MissingDataPolicy::Skip { skipped_records: 2 },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            proto::LayoutState::try_from(result.layout.state),
            Ok(proto::LayoutState::Incomplete)
        );
        assert_eq!(
            result.layout.materialization_stats,
            Some(proto::MaterializationStats {
                expected_records: 3,
                written_records: 1,
                skipped_records: 2,
            })
        );
        assert!(shardpack_manifest::validate_layout(&result.layout).is_ok());
    }
}
