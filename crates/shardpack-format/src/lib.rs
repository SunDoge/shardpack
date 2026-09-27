//! Schema-agnostic, sequential ShardPack container implementation.

mod async_reader;
mod block;
mod checksum;
mod compression;
mod directory;
mod encryption;
mod fingerprint;
mod hashing_reader;
mod reader;
mod reusable_buffer;
mod shard_header;
mod transcode;
mod writer;

pub use async_reader::{AsyncReaderPosition, AsyncShardReader};
pub use block::{BLOCK_HEADER_SIZE, BlockHeader};
pub use compression::BlockCodec;
pub use directory::{RECORD_ENTRY_SIZE, RecordEntry};
pub use encryption::{EncryptionAlgorithm, EncryptionKey};
pub use error::{Error, Result};
pub use fingerprint::ContentFingerprint;
pub use hashing_reader::Blake3Reader;
pub use reader::{ReaderLimits, Record, ShardReader};
pub use shard_header::{SHARD_HEADER_SIZE, ShardHeader};
pub use transcode::{plaintext_block, plaintext_shard_header};
pub use writer::{ShardSummary, ShardWriter, WriterOptions};

pub const FORMAT_VERSION: u16 = 1;

mod error {
    #[derive(Debug, snafu::Snafu)]
    #[snafu(visibility(pub(crate)))]
    pub enum Error {
        #[snafu(display("I/O error: {source}"))]
        Io { source: std::io::Error },
        #[snafu(display("invalid shard header: {reason}"))]
        InvalidShardHeader { reason: &'static str },
        #[snafu(display("invalid block header: {reason}"))]
        InvalidBlockHeader { reason: &'static str },
        #[snafu(display("invalid record directory: {reason}"))]
        InvalidDirectory { reason: &'static str },
        #[snafu(display("unsupported format version {version}"))]
        UnsupportedVersion { version: u16 },
        #[snafu(display("unsupported block codec {codec}"))]
        UnsupportedCodec { codec: u8 },
        #[snafu(display("unsupported encryption algorithm {algorithm}"))]
        UnsupportedEncryption { algorithm: u8 },
        #[snafu(display("encrypted block requires a decryption key"))]
        MissingDecryptionKey,
        #[snafu(display("block authentication failed"))]
        AuthenticationFailed,
        #[snafu(display("random nonce generation failed: {source}"))]
        Random { source: getrandom::Error },
        #[snafu(display("header checksum mismatch"))]
        HeaderChecksumMismatch,
        #[snafu(display("payload checksum mismatch"))]
        PayloadChecksumMismatch,
        #[snafu(display("truncated block"))]
        TruncatedBlock,
        #[snafu(display("reader cannot continue after a parsing error"))]
        ReaderFailed,
        #[snafu(display("invalid record: {reason}"))]
        InvalidRecord { reason: &'static str },
        #[snafu(display("compression error: {source}"))]
        Compression { source: zrip::CompressError },
        #[snafu(display("decompression error: {source}"))]
        Decompression { source: zrip::DecompressError },
        #[snafu(display("block {field} value {actual} exceeds configured limit {limit}"))]
        ResourceLimit {
            field: &'static str,
            actual: u64,
            limit: u64,
        },
    }

    pub type Result<T, E = Error> = std::result::Result<T, E>;
}
