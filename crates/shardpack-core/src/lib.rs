//! Source-agnostic materialization and byte-preserving rebalance services.

use std::io::{Read, Write};

use bytes::Bytes;
use shardpack_format::{ContentFingerprint, ShardReader, ShardSummary, ShardWriter, WriterOptions};
use snafu::Snafu;

mod import;
mod materialize;
mod mirror;
mod rebalance;
mod shuffle;
mod snapshot;
pub use import::{
    ImportOptions, ImportedDataset, import_encrypted_shardpack, import_shardpack,
    import_shardpack_coordinated, import_shardpack_coordinated_with_registry,
    import_shardpack_from_registry,
};
pub use materialize::{
    Materialization, MaterializeEncryption, MaterializeOptions, MissingDataPolicy,
    OfflineShuffleOptions, materialize_layout, materialize_layout_coordinated,
    materialize_snapshot_layout, materialize_snapshot_layout_coordinated,
    materialize_snapshot_split_layout, materialize_snapshot_split_layout_coordinated,
    shard_object_key,
};
pub use mirror::{MirrorResult, mirror_dataset};
pub use rebalance::{
    AsyncSourceReader, RebalanceTransaction, ShardPackLayoutSource, materialize_layout_async,
    rebalance_layout_transaction, rebalance_layout_transaction_with_registry,
};
pub use snapshot::{
    DatasetIdentity, ResolvedDataset, layout_manifest_key, load_layout, load_layout_with_limit,
    load_snapshot, load_snapshot_with_limit, load_trusted_layout, publish_snapshot,
    resolve_dataset, snapshot_manifest_key,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedRecord {
    pub sample_id: [u8; 16],
    pub flags: u32,
    pub payload: Bytes,
}

pub trait SourceReader {
    fn next_record(&mut self) -> Result<Option<EncodedRecord>>;
}

pub trait RecordEncoder<T> {
    fn encode(&self, sample: &T) -> Result<Bytes>;
}

pub trait RecordDecoder {
    type Output;
    fn decode(&self, bytes: Bytes) -> Result<Self::Output>;
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ShardPack format operation failed: {source}"))]
    Format { source: shardpack_format::Error },
    #[snafu(display("record codec failed: {message}"))]
    Codec { message: String },
    #[snafu(display(
        "rebalance changed record count: source {source_count}, target {target_count}"
    ))]
    RecordCountMismatch {
        source_count: u64,
        target_count: u64,
    },
    #[snafu(display("rebalance changed record content"))]
    FingerprintMismatch,
    #[snafu(display("storage operation failed: {source}"))]
    Storage { source: shardpack_storage::Error },
    #[snafu(display("staging operation failed: {source}"))]
    Staging { source: std::io::Error },
    #[snafu(display("uploaded object {key:?} failed identity verification"))]
    ObjectIdentityMismatch { key: String },
    #[snafu(display("invalid materialization option {field}: {reason}"))]
    InvalidOption {
        field: &'static str,
        reason: &'static str,
    },
    #[snafu(display("manifest operation failed: {source}"))]
    Manifest { source: shardpack_manifest::Error },
    #[snafu(display("catalog operation failed: {source}"))]
    Catalog { source: shardpack_catalog::Error },
    #[snafu(display("snapshot and layout are incompatible: {reason}"))]
    SnapshotLayoutMismatch { reason: &'static str },
    #[snafu(display("mirrored dataset failed validation: {reason}"))]
    MirrorMismatch { reason: &'static str },
    #[snafu(display("decryption key ID does not match the source layout"))]
    DecryptionKeyMismatch,
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Default upper bound for authoritative protobuf manifests loaded into
/// memory. Call the explicit `*_with_limit` APIs for unusually large layouts.
pub const DEFAULT_MAX_MANIFEST_BYTES: usize = 512 * 1024 * 1024;

pub struct ShardPackSource<R> {
    reader: ShardReader<R>,
}

impl<R: Read> ShardPackSource<R> {
    pub fn new(input: R) -> Result<Self> {
        Ok(Self {
            reader: ShardReader::new(input).map_err(|source| Error::Format { source })?,
        })
    }
}

impl<R: Read> SourceReader for ShardPackSource<R> {
    fn next_record(&mut self) -> Result<Option<EncodedRecord>> {
        self.reader
            .next_record()
            .map(|record| {
                record.map(|value| EncodedRecord {
                    sample_id: value.sample_id,
                    flags: value.flags,
                    payload: value.payload,
                })
            })
            .map_err(|source| Error::Format { source })
    }
}

pub fn materialize_shard<S: SourceReader, W: Write>(
    source: &mut S,
    output: W,
    options: WriterOptions,
) -> Result<(W, ShardSummary)> {
    let mut writer =
        ShardWriter::new(output, options).map_err(|source| Error::Format { source })?;
    while let Some(record) = source.next_record()? {
        writer
            .add_record(record.sample_id, record.flags, &record.payload)
            .map_err(|source| Error::Format { source })?;
    }
    writer.finish().map_err(|source| Error::Format { source })
}

pub fn rebalance_shard<R: Read, W: Write>(
    input: R,
    output: W,
    options: WriterOptions,
) -> Result<(W, ShardSummary)> {
    let mut source = ShardPackSource::new(input)?;
    let source_fingerprint = drain_fingerprint(&mut source, output, options)?;
    let (output, target) = source_fingerprint.output;
    validate_rebalance(source_fingerprint.source, target.content_fingerprint)?;
    Ok((output, target))
}

struct RepackResult<W> {
    source: ContentFingerprint,
    output: (W, ShardSummary),
}

fn drain_fingerprint<R: Read, W: Write>(
    source: &mut ShardPackSource<R>,
    output: W,
    options: WriterOptions,
) -> Result<RepackResult<W>> {
    let mut fingerprint = ContentFingerprint::default();
    struct TrackingSource<'a, R> {
        inner: &'a mut ShardPackSource<R>,
        fingerprint: &'a mut ContentFingerprint,
    }
    impl<R: Read> SourceReader for TrackingSource<'_, R> {
        fn next_record(&mut self) -> Result<Option<EncodedRecord>> {
            let record = self.inner.next_record()?;
            if let Some(value) = &record {
                self.fingerprint
                    .insert(&value.sample_id, value.flags, &value.payload);
            }
            Ok(record)
        }
    }
    let output = materialize_shard(
        &mut TrackingSource {
            inner: source,
            fingerprint: &mut fingerprint,
        },
        output,
        options,
    )?;
    Ok(RepackResult {
        source: fingerprint,
        output,
    })
}

pub fn validate_rebalance(source: ContentFingerprint, target: ContentFingerprint) -> Result<()> {
    if source.record_count != target.record_count {
        return Err(Error::RecordCountMismatch {
            source_count: source.record_count,
            target_count: target.record_count,
        });
    }
    if source != target {
        return Err(Error::FingerprintMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::rebalance_shard;
    use shardpack_format::{BlockCodec, ShardWriter, WriterOptions};
    use std::io::Cursor;

    #[test]
    fn rebalance_changes_packing_but_preserves_content() {
        let mut original = ShardWriter::new(
            Vec::new(),
            WriterOptions {
                target_block_bytes: 4,
                ..Default::default()
            },
        )
        .unwrap();
        for value in 0_u8..10 {
            original.add_record([value; 16], 0, &[value; 3]).unwrap();
        }
        let (bytes, source_summary) = original.finish().unwrap();
        let options = WriterOptions {
            target_block_bytes: 100,
            codec: BlockCodec::Zstd,
            zstd_level: 1,
        };
        let (rebalanced, target_summary) =
            rebalance_shard(Cursor::new(bytes), Vec::new(), options).unwrap();
        assert_ne!(source_summary.shard_id, target_summary.shard_id);
        assert_eq!(
            source_summary.content_fingerprint,
            target_summary.content_fingerprint
        );
        assert!(!rebalanced.is_empty());
    }
}
