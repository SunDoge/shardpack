use shardpack_format::{BlockCodec, EncryptionKey, ShardReader, ShardWriter, WriterOptions};
use std::{
    hint::black_box,
    io::Cursor,
    time::{Duration, Instant},
};

fn main() {
    const BYTES: usize = 128 * 1024 * 1024;
    const SAMPLE: usize = 64 * 1024;
    for (name, codec, encrypted) in [
        ("none", BlockCodec::None, false),
        ("zstd", BlockCodec::Zstd, false),
        ("encrypted_none", BlockCodec::None, true),
    ] {
        let options = WriterOptions {
            target_block_bytes: 16 * 1024 * 1024,
            codec,
            zstd_level: 1,
        };
        let key = encrypted.then(|| EncryptionKey::new([7; 32]));
        let mut writer = match &key {
            Some(key) => ShardWriter::new_encrypted(Vec::new(), options, key.clone()).unwrap(),
            None => ShardWriter::new(Vec::new(), options).unwrap(),
        };
        let mut sample = vec![42; SAMPLE];
        for index in 0..BYTES / SAMPLE {
            let mut id = [0; 16];
            id[..8].copy_from_slice(&(index as u64).to_le_bytes());
            sample[..8].copy_from_slice(&id[..8]);
            writer.add_record(id, 0, &sample).unwrap();
        }
        let (shard, _) = writer.finish().unwrap();
        let run = || {
            let mut reader = ShardReader::with_limits_and_key(
                Cursor::new(&shard),
                Default::default(),
                key.clone(),
            )
            .unwrap()
            .without_content_fingerprint();
            let mut count = 0;
            while let Some(record) = reader.next_record().unwrap() {
                assert_eq!(record.payload.len(), SAMPLE);
                assert_eq!(&record.payload[..8], &record.sample_id[..8]);
                black_box(record);
                count += 1;
            }
            assert_eq!(count, BYTES / SAMPLE);
        };
        run();
        let started = Instant::now();
        let mut rounds = 0;
        while started.elapsed() < Duration::from_millis(500) {
            run();
            rounds += 1;
        }
        println!(
            "{name},{:.6}",
            rounds as f64 * BYTES as f64 / started.elapsed().as_secs_f64() / 1024_f64.powi(3)
        );
    }
}
