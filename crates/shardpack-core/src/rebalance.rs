use std::path::Path;

use async_trait::async_trait;
use shardpack_catalog::{BarrierLease, Catalog, new_owner_id};
use shardpack_format::{
    AsyncShardReader, Blake3Reader, BlockCodec, ContentFingerprint, ShardWriter, WriterOptions,
};
use shardpack_manifest::{layout_id, proto};
use shardpack_storage::{AsyncObjectReader, Error as StorageError, Store};

use crate::shuffle::AsyncShuffledSource;
use crate::{
    EncodedRecord, Error, Materialization, MaterializeOptions, MissingDataPolicy, Result,
    materialize::{
        completion_fields, manifest_object_key, now_timestamp, proto_fingerprint, publish_layout,
        shard_meta, shard_object_key, validate_options, verify_uploaded_shard, write_record,
    },
    validate_rebalance,
};

#[async_trait]
pub trait AsyncSourceReader {
    async fn next_record(&mut self) -> Result<Option<EncodedRecord>>;
}

pub struct ShardPackLayoutSource {
    store: Store,
    layout: proto::LayoutManifest,
    next_shard: usize,
    current_meta: Option<proto::ShardMeta>,
    current: Option<AsyncShardReader<Blake3Reader<AsyncObjectReader>>>,
    decryption_key: Option<shardpack_format::EncryptionKey>,
}

impl ShardPackLayoutSource {
    pub fn new(store: Store, layout: proto::LayoutManifest) -> Result<Self> {
        shardpack_manifest::validate_layout(&layout)
            .map_err(|source| Error::Manifest { source })?;
        Ok(Self {
            store,
            layout,
            next_shard: 0,
            current_meta: None,
            current: None,
            decryption_key: None,
        })
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

    fn validate_finished_shard(&self) -> Result<()> {
        let reader = self.current.as_ref().expect("current reader exists");
        let meta = self.current_meta.as_ref().expect("current metadata exists");
        let expected = meta
            .content_fingerprint
            .as_ref()
            .expect("validated fingerprint exists");
        let actual = reader.content_fingerprint();
        if reader.records_yielded() != meta.record_count
            || reader.block_count() != meta.block_count
            || actual.record_count != expected.record_count
            || actual.accumulator_a.as_slice() != expected.accumulator_a
            || actual.accumulator_b.as_slice() != expected.accumulator_b
            || reader.input_ref().bytes_read() != meta.byte_size
            || reader.input_ref().digest().as_slice() != meta.shard_id
        {
            return Err(Error::ObjectIdentityMismatch {
                key: meta.object_uri.clone(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncSourceReader for ShardPackLayoutSource {
    async fn next_record(&mut self) -> Result<Option<EncodedRecord>> {
        loop {
            if let Some(reader) = &mut self.current {
                if let Some(record) = reader
                    .next_record()
                    .await
                    .map_err(|source| Error::Format { source })?
                {
                    return Ok(Some(EncodedRecord {
                        sample_id: record.sample_id,
                        flags: record.flags,
                        payload: record.payload,
                    }));
                }
                self.validate_finished_shard()?;
                self.current = None;
                self.current_meta = None;
            }
            let Some(meta) = self.layout.shards.get(self.next_shard).cloned() else {
                return Ok(None);
            };
            self.next_shard += 1;
            let object = self
                .store
                .open_reader(&meta.object_uri)
                .await
                .map_err(|source| Error::Storage { source })?;
            self.current = Some(
                AsyncShardReader::with_limits_and_key(
                    Blake3Reader::new(object),
                    shardpack_format::ReaderLimits::default(),
                    self.decryption_key.clone(),
                )
                .await
                .map_err(|source| Error::Format { source })?,
            );
            self.current_meta = Some(meta);
        }
    }
}

#[derive(Clone, Debug)]
pub struct RebalanceTransaction {
    pub source_layout_id: [u8; 32],
    pub materialization: Materialization,
    pub preferred_event: proto::PreferredLayoutEvent,
    pub retirement: Option<proto::LayoutRetirement>,
}

pub async fn materialize_layout_async<S: AsyncSourceReader + Send>(
    source: &mut S,
    snapshot_id: [u8; 32],
    store: &Store,
    staging_directory: &Path,
    options: &MaterializeOptions,
) -> Result<Materialization> {
    materialize_layout_async_inner(
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

/// Builds all new shards, validates content equivalence, publishes the layout,
/// and only then advances preferred-layout state using catalog CAS.
pub async fn rebalance_layout_transaction(
    store: &Store,
    catalog: &Catalog,
    source_layout: &proto::LayoutManifest,
    staging_directory: &Path,
    options: &MaterializeOptions,
) -> Result<RebalanceTransaction> {
    let lease = catalog
        .acquire_barrier(new_owner_id(), "rebalance-transaction")
        .await
        .map_err(|source| Error::Catalog { source })?;
    let operation = rebalance_layout_transaction_guarded(
        store,
        catalog,
        &lease,
        source_layout,
        staging_directory,
        options,
    )
    .await;
    let release = catalog
        .release_barrier(&lease)
        .await
        .map_err(|source| Error::Catalog { source });
    match operation {
        Err(error) => {
            let _ = release;
            Err(error)
        }
        Ok(transaction) => {
            release?;
            Ok(transaction)
        }
    }
}

async fn rebalance_layout_transaction_guarded(
    store: &Store,
    catalog: &Catalog,
    lease: &BarrierLease,
    source_layout: &proto::LayoutManifest,
    staging_directory: &Path,
    options: &MaterializeOptions,
) -> Result<RebalanceTransaction> {
    shardpack_manifest::validate_layout(source_layout)
        .map_err(|source| Error::Manifest { source })?;
    let expected = fingerprint_from_proto(
        source_layout
            .content_fingerprint
            .as_ref()
            .expect("validated fingerprint"),
    );
    let snapshot_id = source_layout
        .snapshot_id
        .as_slice()
        .try_into()
        .expect("validated snapshot ID");
    let source_layout_id = source_layout
        .layout_id
        .as_slice()
        .try_into()
        .expect("validated layout ID");
    let mut effective = options.clone();
    effective.portability =
        proto::Portability::try_from(source_layout.portability).expect("validated portability");
    effective.durability =
        proto::Durability::try_from(source_layout.durability).expect("validated durability");
    effective.missing_data_policy = source_layout
        .materialization_stats
        .as_ref()
        .filter(|stats| stats.skipped_records > 0)
        .map_or(MissingDataPolicy::Fail, |stats| MissingDataPolicy::Skip {
            skipped_records: stats.skipped_records,
        });
    let mut source = ShardPackLayoutSource::new(store.clone(), source_layout.clone())?;
    if let Some(source_encryption) = &source_layout.encryption {
        let target_encryption = effective.encryption.as_ref().ok_or(Error::InvalidOption {
            field: "encryption",
            reason: "encrypted source requires its decryption key",
        })?;
        if target_encryption.key_id != source_encryption.key_id {
            return Err(Error::InvalidOption {
                field: "encryption.key_id",
                reason: "transactional rebalance requires the source key; use ShardPackLayoutSource for key rotation",
            });
        }
        source =
            source.with_decryption_key(&source_encryption.key_id, target_encryption.key.clone())?;
    }
    let materialization = materialize_layout_async_inner(
        &mut source,
        snapshot_id,
        store,
        staging_directory,
        &effective,
        Some(expected),
        source_layout.split_name.as_deref(),
    )
    .await?;
    let preferred_event = catalog
        .compare_and_switch_preferred_guarded(
            lease,
            &materialization.layout,
            Some(shardpack_manifest::LayoutId(source_layout_id)),
        )
        .await
        .map_err(|source| Error::Catalog { source })?;
    let target_layout_id =
        shardpack_manifest::LayoutId::from_slice(&materialization.layout.layout_id)
            .map_err(|source| Error::Manifest { source })?;
    let retirement = if target_layout_id.0 == source_layout_id {
        None
    } else {
        Some(
            catalog
                .retire_guarded(lease, source_layout, target_layout_id)
                .await
                .map_err(|source| Error::Catalog { source })?,
        )
    };
    Ok(RebalanceTransaction {
        source_layout_id,
        materialization,
        preferred_event,
        retirement,
    })
}

async fn materialize_layout_async_inner<S: AsyncSourceReader + Send>(
    source: &mut S,
    snapshot_id: [u8; 32],
    store: &Store,
    staging_directory: &Path,
    options: &MaterializeOptions,
    expected: Option<ContentFingerprint>,
    split_name: Option<&str>,
) -> Result<Materialization> {
    validate_options(options)?;
    let mut source = AsyncShuffledSource::new(source, options.offline_shuffle)?;
    let mut pending = source.next_record().await?;
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
            let mut records = 0_u64;
            loop {
                let Some(record) = pending.take() else { break };
                let size = record.payload.len() as u64;
                if records > 0 && logical_bytes.saturating_add(size) > options.target_shard_bytes {
                    pending = Some(record);
                    break;
                }
                write_record(&mut writer, &record)?;
                logical_bytes = logical_bytes.saturating_add(size);
                records += 1;
                pending = source.next_record().await?;
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
    if let Some(expected) = expected {
        validate_rebalance(expected, total_fingerprint)?;
    }
    let expected_records = match options.missing_data_policy {
        MissingDataPolicy::Fail => expected.map(|value| value.record_count),
        MissingDataPolicy::Skip { skipped_records } => match expected {
            Some(value) => Some(value.record_count.checked_add(skipped_records).ok_or(
                Error::InvalidOption {
                    field: "missing_data_policy",
                    reason: "record count overflow",
                },
            )?),
            None => None,
        },
    };
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
        created_at: Some(now_timestamp()),
        state,
        split_name: split_name.map(str::to_owned),
        materialization_stats,
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
    let id = layout
        .layout_id
        .as_slice()
        .try_into()
        .expect("validated ID");
    let layout_object_key = manifest_object_key(&options.layout_prefix, &id);
    let layout = publish_layout(store, &layout_object_key, layout).await?;
    Ok(Materialization {
        layout,
        layout_object_key,
    })
}

fn fingerprint_from_proto(value: &proto::ContentFingerprint) -> ContentFingerprint {
    ContentFingerprint {
        record_count: value.record_count,
        accumulator_a: value
            .accumulator_a
            .as_slice()
            .try_into()
            .expect("validated accumulator"),
        accumulator_b: value
            .accumulator_b
            .as_slice()
            .try_into()
            .expect("validated accumulator"),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use shardpack_format::BlockCodec;

    use super::rebalance_layout_transaction;
    use crate::{
        EncodedRecord, MaterializeOptions, MissingDataPolicy, Result, SourceReader,
        materialize_layout,
    };

    struct Records(std::vec::IntoIter<EncodedRecord>);

    impl SourceReader for Records {
        fn next_record(&mut self) -> Result<Option<EncodedRecord>> {
            Ok(self.0.next())
        }
    }

    #[tokio::test]
    async fn validates_before_publishing_and_switches_preferred() {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let records = (0..6_u8)
            .map(|value| EncodedRecord {
                sample_id: [value; 16],
                flags: u32::from(value),
                payload: Bytes::from(vec![value; 10]),
            })
            .collect::<Vec<_>>();
        let mut source = Records(records.into_iter());
        let source_layout = materialize_layout(
            &mut source,
            [8; 32],
            &store,
            staging.path(),
            &MaterializeOptions {
                target_shard_bytes: 20,
                target_block_bytes: 10,
                builder_version: "source".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .layout;
        assert_eq!(source_layout.shards.len(), 3);
        let catalog = shardpack_catalog::Catalog::new(store.clone(), "control").unwrap();
        catalog.switch_preferred(&source_layout).await.unwrap();

        let transaction = rebalance_layout_transaction(
            &store,
            &catalog,
            &source_layout,
            staging.path(),
            &MaterializeOptions {
                target_shard_bytes: 100,
                target_block_bytes: 100,
                block_codec: BlockCodec::Zstd,
                builder_version: "rebalance".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(transaction.materialization.layout.shards.len(), 1);
        assert_eq!(
            transaction.materialization.layout.content_fingerprint,
            source_layout.content_fingerprint
        );
        assert_eq!(transaction.preferred_event.generation, 2);
        assert_eq!(
            transaction.retirement.as_ref().unwrap().layout_id,
            source_layout.layout_id
        );
        assert_eq!(
            transaction.preferred_event.previous_layout_id,
            Some(source_layout.layout_id)
        );
        assert_eq!(
            catalog
                .preferred(shardpack_manifest::SnapshotId([8; 32]))
                .await
                .unwrap()
                .unwrap()
                .layout_id,
            transaction.materialization.layout.layout_id
        );
    }

    #[tokio::test]
    async fn rebalance_preserves_incomplete_counts_and_state() {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let mut source = Records(
            vec![EncodedRecord {
                sample_id: [3; 16],
                flags: 0,
                payload: Bytes::from_static(b"present"),
            }]
            .into_iter(),
        );
        let source_layout = materialize_layout(
            &mut source,
            [3; 32],
            &store,
            staging.path(),
            &MaterializeOptions {
                missing_data_policy: MissingDataPolicy::Skip { skipped_records: 4 },
                builder_version: "incomplete-source".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .layout;
        let catalog = shardpack_catalog::Catalog::new(store.clone(), "control").unwrap();
        catalog.switch_preferred(&source_layout).await.unwrap();
        let transaction = rebalance_layout_transaction(
            &store,
            &catalog,
            &source_layout,
            staging.path(),
            &MaterializeOptions {
                builder_version: "incomplete-target".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            transaction.materialization.layout.state,
            shardpack_manifest::proto::LayoutState::Incomplete as i32
        );
        assert_eq!(
            transaction.materialization.layout.materialization_stats,
            source_layout.materialization_stats
        );
    }
}
