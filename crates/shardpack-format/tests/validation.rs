use std::io::Cursor;

use shardpack_format::{
    AsyncShardReader, BLOCK_HEADER_SIZE, BlockHeader, ContentFingerprint, Error, RECORD_ENTRY_SIZE,
    ReaderLimits, RecordEntry, SHARD_HEADER_SIZE, ShardReader, ShardWriter, WriterOptions,
};

fn malformed_directory() -> Vec<u8> {
    let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
    writer.add_record([1; 16], 7, b"first").unwrap();
    writer.add_record([2; 16], 9, b"second").unwrap();
    let mut bytes = writer.finish().unwrap().0;
    let body = SHARD_HEADER_SIZE + BLOCK_HEADER_SIZE;
    let size = body + RECORD_ENTRY_SIZE + 16;
    bytes[size..size + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    let mut header =
        BlockHeader::decode(bytes[SHARD_HEADER_SIZE..body].try_into().unwrap()).unwrap();
    header.payload_crc32c = crc32c::crc32c(&bytes[body..]);
    bytes[SHARD_HEADER_SIZE..body].copy_from_slice(&header.encode());
    bytes
}

#[tokio::test]
async fn directory_error_is_terminal_and_does_not_publish_partial_records() {
    let bytes = malformed_directory();
    let mut sync = ShardReader::new(Cursor::new(&bytes)).unwrap();
    let mut asynchronous = AsyncShardReader::new(Cursor::new(&bytes)).await.unwrap();
    assert!(matches!(
        sync.next_record(),
        Err(Error::InvalidDirectory { .. })
    ));
    assert!(matches!(
        asynchronous.next_record().await,
        Err(Error::InvalidDirectory { .. })
    ));
    for _ in 0..2 {
        assert!(matches!(sync.next_record(), Err(Error::ReaderFailed)));
        assert!(matches!(
            asynchronous.next_record().await,
            Err(Error::ReaderFailed)
        ));
    }
    assert_eq!(sync.content_fingerprint(), ContentFingerprint::default());
    assert_eq!(
        asynchronous.content_fingerprint(),
        ContentFingerprint::default()
    );
    assert_eq!(asynchronous.buffered_records(), 0);
}

#[test]
fn directory_supports_lengths_above_u32_without_allocating_payloads() {
    for size in [u64::from(u32::MAX) + 1, u64::MAX] {
        let entry = RecordEntry {
            sample_id: [0xab; 16],
            payload_size: size,
            flags: 0x12345678,
        };
        let bytes = entry.encode();
        assert_eq!(bytes.len(), 28);
        assert_eq!(&bytes[16..24], &size.to_le_bytes());
        assert_eq!(&bytes[24..], &0x12345678_u32.to_le_bytes());
        assert_eq!(RecordEntry::decode(&bytes).unwrap(), entry);
    }
}

#[test]
fn shared_header_validation_rejects_impossible_and_excessive_lengths() {
    let bytes = malformed_directory();
    let mut header = BlockHeader::decode(bytes[64..128].try_into().unwrap()).unwrap();
    let limits = ReaderLimits::default();
    limits.validate_header(&header).unwrap();
    header.stored_body_bytes = u64::MAX;
    assert!(matches!(
        limits.validate_header(&header),
        Err(Error::ResourceLimit { .. })
    ));
    header.stored_body_bytes = 1;
    assert!(matches!(
        limits.validate_header(&header),
        Err(Error::InvalidBlockHeader { .. })
    ));
    header.stored_body_bytes = 100;
    assert!(matches!(
        limits.validate_header(&header),
        Err(Error::InvalidBlockHeader { .. })
    ));
    header.directory_bytes = 1;
    assert!(matches!(
        limits.validate_header(&header),
        Err(Error::InvalidDirectory { .. })
    ));
}
