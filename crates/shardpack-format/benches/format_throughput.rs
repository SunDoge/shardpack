use std::{hint::black_box, io::Cursor, time::Instant};

use shardpack_format::{BlockCodec, EncryptionKey, ShardReader, ShardWriter, WriterOptions};

const LOGICAL_BYTES: usize = 128 * 1024 * 1024;
const RECORD_BYTES: usize = 256 * 1024;
const READ_ROUNDS: usize = 3;

fn main() {
    let payload = vec![0x5a; RECORD_BYTES];
    let records = LOGICAL_BYTES / RECORD_BYTES;
    run_codec("none", BlockCodec::None, &payload, records);
    run_codec("zstd", BlockCodec::Zstd, &payload, records);
    run_encrypted(&payload, records);
}

fn run_codec(name: &str, codec: BlockCodec, payload: &[u8], records: usize) {
    let started = Instant::now();
    let mut writer = ShardWriter::new(
        Vec::with_capacity(LOGICAL_BYTES),
        WriterOptions {
            target_block_bytes: 16 * 1024 * 1024,
            codec,
            zstd_level: 1,
        },
    )
    .unwrap();
    for index in 0..records {
        let mut sample_id = [0_u8; 16];
        sample_id[..8].copy_from_slice(&(index as u64).to_le_bytes());
        writer.add_record(sample_id, 0, payload).unwrap();
    }
    let (shard, _) = writer.finish().unwrap();
    let write_elapsed = started.elapsed();

    let started = Instant::now();
    for _ in 0..READ_ROUNDS {
        let mut reader = ShardReader::new(Cursor::new(&shard)).unwrap();
        while let Some(record) = reader.next_record().unwrap() {
            black_box(record.payload);
        }
    }
    let read_elapsed = started.elapsed();

    let started = Instant::now();
    for _ in 0..READ_ROUNDS {
        let mut reader = ShardReader::new(Cursor::new(&shard))
            .unwrap()
            .without_content_fingerprint();
        while let Some(record) = reader.next_record().unwrap() {
            black_box(record.payload);
        }
    }
    let training_read_elapsed = started.elapsed();

    report(&format!("write {name}"), LOGICAL_BYTES, write_elapsed);
    report(
        &format!("read {name}"),
        LOGICAL_BYTES * READ_ROUNDS,
        read_elapsed,
    );
    report(
        &format!("training read {name}"),
        LOGICAL_BYTES * READ_ROUNDS,
        training_read_elapsed,
    );
    println!(
        "{name} stored ratio: {:.4}",
        shard.len() as f64 / LOGICAL_BYTES as f64
    );
}

fn run_encrypted(payload: &[u8], records: usize) {
    let key = EncryptionKey::new([0x5a; 32]);
    let started = Instant::now();
    let mut writer = ShardWriter::new_encrypted(
        Vec::with_capacity(LOGICAL_BYTES),
        WriterOptions {
            target_block_bytes: 16 * 1024 * 1024,
            codec: BlockCodec::None,
            zstd_level: 1,
        },
        key.clone(),
    )
    .unwrap();
    for index in 0..records {
        let mut sample_id = [0_u8; 16];
        sample_id[..8].copy_from_slice(&(index as u64).to_le_bytes());
        writer.add_record(sample_id, 0, payload).unwrap();
    }
    let (shard, _) = writer.finish().unwrap();
    let write_elapsed = started.elapsed();

    let started = Instant::now();
    for _ in 0..READ_ROUNDS {
        let mut reader = ShardReader::new_encrypted(Cursor::new(&shard), key.clone())
            .unwrap()
            .without_content_fingerprint();
        while let Some(record) = reader.next_record().unwrap() {
            black_box(record.payload);
        }
    }
    let read_elapsed = started.elapsed();

    report("write encrypted", LOGICAL_BYTES, write_elapsed);
    report(
        "training read encrypted",
        LOGICAL_BYTES * READ_ROUNDS,
        read_elapsed,
    );
}

fn report(name: &str, bytes: usize, elapsed: std::time::Duration) {
    let gib_per_second = bytes as f64 / elapsed.as_secs_f64() / 1024_f64.powi(3);
    println!("{name}: {gib_per_second:.2} GiB/s ({elapsed:?})");
}
