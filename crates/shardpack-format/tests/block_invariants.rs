//! Writer block-boundary invariants swept across target sizes, codecs, and
//! payload shapes, plus boundary-resume position semantics.

use shardpack_format::{
    BLOCK_HEADER_SIZE, BlockCodec, BlockHeader, EncryptionKey, ReaderLimits, SHARD_HEADER_SIZE,
    ShardReader, ShardWriter, WriterOptions,
};
use std::io::Cursor;

fn blocks(bytes: &[u8]) -> Vec<BlockHeader> {
    let mut headers = Vec::new();
    let mut offset = SHARD_HEADER_SIZE;
    while offset < bytes.len() {
        let raw: [u8; BLOCK_HEADER_SIZE] = bytes[offset..offset + BLOCK_HEADER_SIZE]
            .try_into()
            .unwrap();
        let header = BlockHeader::decode(&raw).unwrap();
        offset += BLOCK_HEADER_SIZE + header.stored_body_bytes as usize;
        headers.push(header);
    }
    assert_eq!(offset, bytes.len(), "blocks must tile the shard exactly");
    headers
}

fn round_trip(target: usize, codec: BlockCodec, records: &[(u32, usize)]) -> Vec<BlockHeader> {
    let mut writer = ShardWriter::new(
        Vec::new(),
        WriterOptions {
            target_block_bytes: target,
            codec,
            zstd_level: 1,
        },
    )
    .unwrap();
    let mut expected = Vec::new();
    for (index, (flags, size)) in records.iter().enumerate() {
        let payload = vec![(index % 251) as u8; *size];
        writer
            .add_record([(index % 256) as u8; 16], *flags, &payload)
            .unwrap();
        expected.push((*flags, payload));
    }
    let (bytes, summary) = writer.finish().unwrap();
    let headers = blocks(&bytes);
    assert_eq!(
        summary.block_count as usize,
        headers.len(),
        "summary block count must match physical blocks (target={target})"
    );

    let mut reader =
        ShardReader::with_limits(Cursor::new(&bytes), ReaderLimits::default()).unwrap();
    let mut decoded = Vec::new();
    while let Some(record) = reader.next_record().unwrap() {
        decoded.push((record.flags, record.payload.to_vec()));
    }
    assert_eq!(
        reader.content_fingerprint(),
        summary.content_fingerprint,
        "fingerprint must round-trip (target={target})"
    );
    assert_eq!(
        decoded, expected,
        "payloads must round-trip (target={target})"
    );
    headers
}

#[test]
fn block_invariants_hold_across_target_and_payload_sweeps() {
    for codec in [BlockCodec::None, BlockCodec::Zstd] {
        for target in [
            0usize, 1, 27, 28, 29, 55, 56, 57, 83, 84, 85, 112, 500, 4096,
        ] {
            for payloads in [
                vec![0usize],
                vec![0, 0, 0],
                vec![1],
                vec![target.saturating_sub(28)],
                vec![target.saturating_sub(28) + 1],
                vec![target.saturating_sub(27)],
                vec![28, 0, 1, 0],
                vec![100, 100, 100],
                vec![0; 40],
            ] {
                let records: Vec<(u32, usize)> = payloads
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (i as u32, *p))
                    .collect();
                let headers = round_trip(target, codec, &records);
                for header in &headers {
                    let directory = header.record_count as usize * 28;
                    let body = header.stored_body_bytes as usize;
                    // Invariant 1: directory matches record count.
                    assert_eq!(header.directory_bytes as usize, directory);
                    // Invariant 2: multi-record blocks never exceed target
                    // (raw directory + raw payload).
                    if header.record_count > 1 {
                        let raw = directory + header.raw_payload_bytes as usize;
                        assert!(
                            raw <= target,
                            "multi-record block raw size {raw} exceeds target {target}"
                        );
                    }
                    // Invariant 3: jumbo flag iff single record exceeding target.
                    let raw = directory + header.raw_payload_bytes as usize;
                    let jumbo = header.flags & 1 == 1;
                    assert_eq!(
                        jumbo,
                        header.record_count == 1 && raw > target,
                        "jumbo flag must mark exactly single-record blocks over target"
                    );
                    // Invariant 4: physical body matches directory + stored + tag.
                    let tag =
                        if header.encryption == shardpack_format::EncryptionAlgorithm::Aes256Gcm {
                            16
                        } else {
                            0
                        };
                    assert!(body >= directory + tag);
                }
            }
        }
    }
}

#[test]
fn empty_shard_round_trips() {
    let writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
    let (bytes, summary) = writer.finish().unwrap();
    assert_eq!(bytes.len(), SHARD_HEADER_SIZE);
    assert_eq!(summary.record_count, 0);
    assert_eq!(summary.block_count, 0);
    let mut reader = ShardReader::new(Cursor::new(&bytes)).unwrap();
    assert!(reader.next_record().unwrap().is_none());
    assert_eq!(reader.content_fingerprint(), summary.content_fingerprint);
}

#[test]
fn plaintext_shard_ignores_a_supplied_key() {
    let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
    writer.add_record([1; 16], 0, b"x").unwrap();
    let (bytes, _) = writer.finish().unwrap();
    // A plaintext shard offered with a key: sync reader must not fail-closed
    // spuriously (it ignores the key).
    let mut reader = ShardReader::with_limits_and_key(
        Cursor::new(&bytes),
        ReaderLimits::default(),
        Some(EncryptionKey::new([9; 32])),
    )
    .unwrap();
    assert!(reader.next_record().unwrap().is_some());
}

#[tokio::test]
async fn position_before_the_first_block_read_matches_constructor_coordinates() {
    use shardpack_format::AsyncShardReader;

    // Build a shard with three blocks so block 1 is a mid-shard resume point.
    let mut writer = ShardWriter::new(
        Vec::new(),
        WriterOptions {
            target_block_bytes: 2 * (28 + 3),
            codec: BlockCodec::None,
            zstd_level: 1,
        },
    )
    .unwrap();
    for value in 1..=6_u8 {
        writer.add_record([value; 16], 0, &[value; 3]).unwrap();
    }
    let (bytes, summary) = writer.finish().unwrap();
    assert_eq!(summary.block_count, 3);

    // Locate the physical start of block 1.
    let mut offset = SHARD_HEADER_SIZE;
    let mut block_starts = Vec::new();
    while offset < bytes.len() {
        block_starts.push(offset);
        let raw: [u8; BLOCK_HEADER_SIZE] = bytes[offset..offset + BLOCK_HEADER_SIZE]
            .try_into()
            .unwrap();
        let header = BlockHeader::decode(&raw).unwrap();
        offset += BLOCK_HEADER_SIZE + header.stored_body_bytes as usize;
    }

    let reader = AsyncShardReader::from_block_boundary(
        Cursor::new(bytes.clone()),
        ReaderLimits::default(),
        1,
        block_starts[1] as u64,
    );
    let position = reader.position();
    assert_eq!(
        position.block_index, 1,
        "block index must match the resume point"
    );
    assert_eq!(
        position.block_start_offset, block_starts[1] as u64,
        "block offset must match the resume point"
    );
    assert_eq!(position.record_index, 0);
    assert_eq!(position.next_block_offset, block_starts[1] as u64);
}
