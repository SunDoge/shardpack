use std::io::Cursor;

use shardpack_format::{BlockCodec, ShardReader, ShardWriter, WriterOptions};

fn golden_shard() -> Vec<u8> {
    let mut writer = ShardWriter::new(
        Vec::new(),
        WriterOptions {
            target_block_bytes: 1024,
            codec: BlockCodec::None,
            zstd_level: 3,
        },
    )
    .unwrap();
    writer
        .add_record(
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            0x1122_3344,
            b"alpha",
        )
        .unwrap();
    writer
        .add_record(
            [
                255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240,
            ],
            0xaabb_ccdd,
            &[0, 1, 2, 3],
        )
        .unwrap();
    writer.finish().unwrap().0
}

fn golden_bytes() -> Vec<u8> {
    let compact = include_str!("data/v1_none.hex")
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    hex::decode(compact).unwrap()
}

#[test]
fn v1_none_wire_bytes_are_stable() {
    assert_eq!(hex::encode(golden_shard()), hex::encode(golden_bytes()));
}

#[test]
fn every_truncation_is_rejected() {
    let bytes = golden_bytes();
    for cut in 0..bytes.len() {
        let Ok(mut reader) = ShardReader::new(Cursor::new(&bytes[..cut])) else {
            continue;
        };
        let mut rejected = false;
        loop {
            match reader.next_record() {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {
                    rejected = true;
                    break;
                }
            }
        }
        // With no footer, a cut exactly at a block boundary is intentionally
        // indistinguishable from a valid shorter stream. This fixture has one
        // block, so the shard-header boundary is the sole valid prefix.
        assert!(
            rejected || cut == shardpack_format::SHARD_HEADER_SIZE,
            "truncation at byte {cut} was accepted"
        );
    }
}

#[test]
fn every_single_bit_corruption_is_rejected() {
    let original = golden_bytes();
    for byte_index in 0..original.len() {
        for bit in 0..8 {
            let mut damaged = original.clone();
            damaged[byte_index] ^= 1 << bit;
            let accepted = ShardReader::new(Cursor::new(damaged)).is_ok_and(|mut reader| {
                loop {
                    match reader.next_record() {
                        Ok(Some(_)) => {}
                        Ok(None) => return true,
                        Err(_) => return false,
                    }
                }
            });
            assert!(
                !accepted,
                "bit {bit} of byte {byte_index} was not protected"
            );
        }
    }
}

#[test]
fn generated_record_sets_round_trip_for_both_codecs() {
    for codec in [BlockCodec::None, BlockCodec::Zstd] {
        for seed in 0..32_u8 {
            let mut writer = ShardWriter::new(
                Vec::new(),
                WriterOptions {
                    target_block_bytes: usize::from(seed % 11 + 1),
                    codec,
                    zstd_level: 1,
                },
            )
            .unwrap();
            let expected = (0..usize::from(seed % 13 + 1))
                .map(|index| {
                    let size = (index * 17 + usize::from(seed)) % 37;
                    let payload = (0..size)
                        .map(|offset| seed.wrapping_add(index as u8).wrapping_add(offset as u8))
                        .collect::<Vec<_>>();
                    let sample_id = [seed.wrapping_add(index as u8); 16];
                    writer
                        .add_record(sample_id, index as u32, &payload)
                        .unwrap();
                    (sample_id, index as u32, payload)
                })
                .collect::<Vec<_>>();
            let (bytes, summary) = writer.finish().unwrap();
            let mut reader = ShardReader::new(Cursor::new(bytes)).unwrap();
            let mut actual = Vec::new();
            while let Some(record) = reader.next_record().unwrap() {
                actual.push((record.sample_id, record.flags, record.payload.to_vec()));
            }
            assert_eq!(actual, expected);
            assert_eq!(reader.content_fingerprint(), summary.content_fingerprint);
            assert_eq!(reader.block_count(), summary.block_count);
        }
    }
}
