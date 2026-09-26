//! Compare dependency versions with identical data and this identical harness.
use shardpack_format::{BlockCodec, ShardReader, ShardWriter, WriterOptions};
use std::{
    hint::black_box,
    io::Cursor,
    time::{Duration, Instant},
};

const SIZE: usize = 16 * 1024 * 1024;

fn measure(name: &str, level: i32, operation: &str, stored: usize, mut run: impl FnMut()) {
    run(); // warm caches and reusable contexts
    let start = Instant::now();
    let mut rounds = 0;
    while start.elapsed() < Duration::from_millis(200) {
        run();
        rounds += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "{name},{level},{operation},{stored},{:.6}",
        SIZE as f64 * rounds as f64 / elapsed / 1024_f64.powi(3)
    );
}

fn compare(name: &str, data: &[u8]) {
    for level in [1, 3] {
        let mut encoder = zrip::CompressContext::new(level).unwrap();
        encoder.set_content_checksum(false);
        let encoded = encoder.compress(data).unwrap().into_owned();
        assert_eq!(zstd::bulk::decompress(&encoded, SIZE).unwrap(), data);
        measure(name, level, "encode", encoded.len(), || {
            black_box(encoder.compress(black_box(data)).unwrap());
        });

        // Both zrip versions decode the same C-zstd frame, isolating decoder changes.
        let reference = zstd::bulk::compress(data, level).unwrap();
        let mut decoder = zrip::DecompressContext::new();
        let mut output = Vec::with_capacity(SIZE);
        decoder
            .decompress_into_with_limit(&reference, &mut output, SIZE)
            .unwrap();
        assert_eq!(output, data); // validation outside the measured region
        measure(name, level, "decode_reused_buffer", reference.len(), || {
            output.clear();
            decoder
                .decompress_into_with_limit(black_box(&reference), &mut output, SIZE)
                .unwrap();
            black_box(&output);
        });
        measure(name, level, "decode_fresh_buffer", reference.len(), || {
            let mut output = Vec::with_capacity(SIZE);
            decoder
                .decompress_into_with_limit(black_box(&reference), &mut output, SIZE)
                .unwrap();
            black_box(output);
        });

        let mut writer = ShardWriter::new(
            Vec::new(),
            WriterOptions {
                target_block_bytes: SIZE,
                codec: BlockCodec::Zstd,
                zstd_level: level,
            },
        )
        .unwrap();
        for (index, chunk) in data.chunks(64 * 1024).enumerate() {
            let mut id = [0; 16];
            id[..8].copy_from_slice(&(index as u64).to_le_bytes());
            writer.add_record(id, 0, chunk).unwrap();
        }
        let shard = writer.finish().unwrap().0;
        measure(name, level, "spk_training_read", shard.len(), || {
            let mut reader = ShardReader::new(Cursor::new(&shard))
                .unwrap()
                .without_content_fingerprint();
            let mut bytes = 0;
            while let Some(record) = reader.next_record().unwrap() {
                bytes += record.payload.len();
                black_box(record);
            }
            assert_eq!(bytes, SIZE);
        });
    }
}

fn main() {
    let mut structured = Vec::with_capacity(SIZE);
    let template = br#"{"sample_id":"0123456789abcdef","class":17,"caption":"warehouse item","values":[1,2,3,4,5,6,7,8]}\n"#;
    while structured.len() < SIZE {
        structured.extend_from_slice(template);
    }
    structured.truncate(SIZE);
    for (index, byte) in structured.iter_mut().step_by(97).enumerate() {
        *byte ^= index as u8;
    }
    compare("structured", &structured);

    let mut random = vec![0; SIZE];
    let mut state = 0x1234_5678_9abc_def0_u64;
    for chunk in random.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }
    compare("random", &random);

    if let Some(path) = std::env::args_os().skip(1).find(|arg| arg != "--bench") {
        let mut files = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "jpg"))
            .collect::<Vec<_>>();
        files.sort();
        let mut jpeg = Vec::new();
        for path in files {
            jpeg.extend(std::fs::read(path).unwrap());
            if jpeg.len() >= SIZE {
                break;
            }
        }
        assert!(jpeg.len() >= SIZE, "need at least 16 MiB of JPEG input");
        jpeg.truncate(SIZE);
        compare("coco_jpeg", &jpeg);
    }
}
