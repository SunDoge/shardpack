use std::{hint::black_box, time::Instant};

const BYTES: usize = 64 * 1024 * 1024;
const ROUNDS: usize = 3;

fn main() {
    let repeated = vec![0x5a; BYTES];
    let mut structured = Vec::with_capacity(BYTES);
    let template = br#"{"sample_id":"0123456789abcdef","class":17,"caption":"warehouse item","values":[1,2,3,4,5,6,7,8]}\n"#;
    while structured.len() < BYTES {
        structured.extend_from_slice(template);
    }
    structured.truncate(BYTES);
    for (index, byte) in structured.iter_mut().step_by(97).enumerate() {
        *byte ^= index as u8;
    }
    let mut random = vec![0_u8; BYTES];
    let mut state = 0x1234_5678_9abc_def0_u64;
    for chunk in random.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }

    for (name, input) in [
        ("repeated", repeated.as_slice()),
        ("structured", structured.as_slice()),
        ("random", random.as_slice()),
    ] {
        println!("{name}:");
        compare(input, 1);
        compare(input, 3);
    }
}

fn compare(input: &[u8], level: i32) {
    let started = Instant::now();
    let mut zrip_encoded = Vec::new();
    for _ in 0..ROUNDS {
        zrip_encoded = zrip::compress(black_box(input), level).unwrap();
        black_box(&zrip_encoded);
    }
    report(
        "zrip encode",
        input.len() * ROUNDS,
        started.elapsed(),
        input,
        &zrip_encoded,
        level,
    );

    let started = Instant::now();
    let mut c_encoded = Vec::new();
    for _ in 0..ROUNDS {
        c_encoded = zstd::bulk::compress(black_box(input), level).unwrap();
        black_box(&c_encoded);
    }
    report(
        "C zstd encode",
        input.len() * ROUNDS,
        started.elapsed(),
        input,
        &c_encoded,
        level,
    );

    for (name, encoded) in [("zrip frame", &zrip_encoded), ("C zstd frame", &c_encoded)] {
        let started = Instant::now();
        for _ in 0..ROUNDS {
            let decoded = zrip::decompress_with_limit(black_box(encoded), input.len()).unwrap();
            assert_eq!(decoded, input);
            black_box(decoded);
        }
        let gib_per_second =
            input.len() as f64 * ROUNDS as f64 / started.elapsed().as_secs_f64() / 1024_f64.powi(3);
        println!("  zrip one-shot decode {name} L{level}: {gib_per_second:.2} GiB/s");

        let mut context = zrip::DecompressContext::new();
        let started = Instant::now();
        for _ in 0..ROUNDS {
            let decoded = context
                .decompress_with_limit(black_box(encoded), input.len())
                .unwrap();
            assert_eq!(&*decoded, input);
            black_box(decoded);
        }
        let gib_per_second =
            input.len() as f64 * ROUNDS as f64 / started.elapsed().as_secs_f64() / 1024_f64.powi(3);
        println!("  zrip context decode {name} L{level}: {gib_per_second:.2} GiB/s");

        let mut context = zrip::DecompressContext::new();
        let started = Instant::now();
        for _ in 0..ROUNDS {
            let decoded = context
                .decompress_with_limit(black_box(encoded), input.len())
                .unwrap()
                .into_owned();
            assert_eq!(decoded, input);
            black_box(decoded);
        }
        let gib_per_second =
            input.len() as f64 * ROUNDS as f64 / started.elapsed().as_secs_f64() / 1024_f64.powi(3);
        println!("  zrip context + owned {name} L{level}: {gib_per_second:.2} GiB/s");

        let mut context = zrip::DecompressContext::new();
        let started = Instant::now();
        for _ in 0..ROUNDS {
            let mut decoded = Vec::with_capacity(input.len());
            let written = context
                .decompress_into_with_limit(black_box(encoded), &mut decoded, input.len())
                .unwrap();
            assert_eq!(written, input.len());
            assert_eq!(decoded, input);
            black_box(decoded);
        }
        let gib_per_second =
            input.len() as f64 * ROUNDS as f64 / started.elapsed().as_secs_f64() / 1024_f64.powi(3);
        println!("  zrip context + caller buffer {name} L{level}: {gib_per_second:.2} GiB/s");

        let mut c_context = zstd::bulk::Decompressor::new().unwrap();
        let started = Instant::now();
        for _ in 0..ROUNDS {
            let decoded = c_context
                .decompress(black_box(encoded), input.len())
                .unwrap();
            assert_eq!(decoded, input);
            black_box(decoded);
        }
        let gib_per_second =
            input.len() as f64 * ROUNDS as f64 / started.elapsed().as_secs_f64() / 1024_f64.powi(3);
        println!("  C context decode {name} L{level}: {gib_per_second:.2} GiB/s");

        let started = Instant::now();
        for _ in 0..ROUNDS {
            let decoded = zstd::bulk::decompress(black_box(encoded), input.len()).unwrap();
            assert_eq!(decoded, input);
            black_box(decoded);
        }
        let gib_per_second =
            input.len() as f64 * ROUNDS as f64 / started.elapsed().as_secs_f64() / 1024_f64.powi(3);
        println!("  C decode {name} L{level}: {gib_per_second:.2} GiB/s");
    }
}

fn report(
    name: &str,
    bytes: usize,
    elapsed: std::time::Duration,
    input: &[u8],
    encoded: &[u8],
    level: i32,
) {
    let gib_per_second = bytes as f64 / elapsed.as_secs_f64() / 1024_f64.powi(3);
    let ratio = encoded.len() as f64 / input.len() as f64;
    println!("  {name} L{level}: {gib_per_second:.2} GiB/s, ratio {ratio:.4}");
}
