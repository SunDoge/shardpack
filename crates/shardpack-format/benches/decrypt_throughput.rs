use std::{hint::black_box, time::Instant};

use aes_gcm::{
    Aes256Gcm,
    aead::{AeadInOut, KeyInit, Nonce},
};
use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce as AwsNonce, UnboundKey};
use chacha20poly1305::ChaCha20Poly1305;

const BLOCK_BYTES: usize = 16 * 1024 * 1024;
const BLOCKS: usize = 8;
const ROUNDS: usize = 4;

fn main() {
    let input = vec![0x5a; BLOCK_BYTES];
    let aad = b"shardpack-block-header-and-directory";

    let aes = Aes256Gcm::new_from_slice(&[0x11; 32]).unwrap();
    let mut aes_blocks = vec![input.clone(); BLOCKS];
    let mut aes_tags = aes_blocks
        .iter_mut()
        .enumerate()
        .map(|(index, block)| {
            let nonce = Nonce::<Aes256Gcm>::from(bench_nonce(0, index));
            aes.encrypt_inout_detached(&nonce, aad, block.as_mut_slice().into())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut aes_elapsed = std::time::Duration::ZERO;
    for round in 0..ROUNDS {
        let start = Instant::now();
        for (index, (block, tag)) in aes_blocks.iter_mut().zip(&aes_tags).enumerate() {
            let nonce = Nonce::<Aes256Gcm>::from(bench_nonce(round, index));
            aes.decrypt_inout_detached(&nonce, aad, block.as_mut_slice().into(), tag)
                .unwrap();
            black_box(&*block);
        }
        aes_elapsed += start.elapsed();
        for (index, (block, tag)) in aes_blocks.iter_mut().zip(&mut aes_tags).enumerate() {
            let nonce = Nonce::<Aes256Gcm>::from(bench_nonce(round + 1, index));
            *tag = aes
                .encrypt_inout_detached(&nonce, aad, block.as_mut_slice().into())
                .unwrap();
        }
    }

    let chacha = ChaCha20Poly1305::new_from_slice(&[0x33; 32]).unwrap();
    let mut chacha_blocks = vec![input; BLOCKS];
    let mut chacha_tags = chacha_blocks
        .iter_mut()
        .enumerate()
        .map(|(index, block)| {
            let nonce = Nonce::<ChaCha20Poly1305>::from(bench_nonce(0, index));
            chacha
                .encrypt_inout_detached(&nonce, aad, block.as_mut_slice().into())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut chacha_elapsed = std::time::Duration::ZERO;
    for round in 0..ROUNDS {
        let start = Instant::now();
        for (index, (block, tag)) in chacha_blocks.iter_mut().zip(&chacha_tags).enumerate() {
            let nonce = Nonce::<ChaCha20Poly1305>::from(bench_nonce(round, index));
            chacha
                .decrypt_inout_detached(&nonce, aad, block.as_mut_slice().into(), tag)
                .unwrap();
            black_box(&*block);
        }
        chacha_elapsed += start.elapsed();
        for (index, (block, tag)) in chacha_blocks.iter_mut().zip(&mut chacha_tags).enumerate() {
            let nonce = Nonce::<ChaCha20Poly1305>::from(bench_nonce(round + 1, index));
            *tag = chacha
                .encrypt_inout_detached(&nonce, aad, block.as_mut_slice().into())
                .unwrap();
        }
    }

    let aws = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &[0x55; 32]).unwrap());
    let mut aws_blocks = vec![vec![0x5a; BLOCK_BYTES]; BLOCKS];
    let mut aws_tags = aws_blocks
        .iter_mut()
        .enumerate()
        .map(|(index, block)| {
            aws.seal_in_place_separate_tag(
                AwsNonce::assume_unique_for_key(bench_nonce(0, index)),
                Aad::from(aad),
                block,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut aws_elapsed = std::time::Duration::ZERO;
    for round in 0..ROUNDS {
        let start = Instant::now();
        for (index, (block, tag)) in aws_blocks.iter_mut().zip(&aws_tags).enumerate() {
            aws.open_in_place_separate_tag(
                AwsNonce::assume_unique_for_key(bench_nonce(round, index)),
                Aad::from(aad),
                tag.as_ref(),
                block,
            )
            .unwrap();
            black_box(&*block);
        }
        aws_elapsed += start.elapsed();
        for (index, (block, tag)) in aws_blocks.iter_mut().zip(&mut aws_tags).enumerate() {
            *tag = aws
                .seal_in_place_separate_tag(
                    AwsNonce::assume_unique_for_key(bench_nonce(round + 1, index)),
                    Aad::from(aad),
                    block,
                )
                .unwrap();
        }
    }

    report("AES-256-GCM", aes_elapsed);
    report("ChaCha20-Poly1305", chacha_elapsed);
    report("AWS-LC AES-256-GCM", aws_elapsed);
}

fn bench_nonce(round: usize, index: usize) -> [u8; 12] {
    let mut nonce = [0_u8; 12];
    nonce[..8].copy_from_slice(&(round as u64).to_le_bytes());
    nonce[8..].copy_from_slice(&(index as u32).to_le_bytes());
    nonce
}

fn report(name: &str, elapsed: std::time::Duration) {
    let bytes = (BLOCK_BYTES * BLOCKS * ROUNDS) as f64;
    let gib_per_second = bytes / elapsed.as_secs_f64() / (1024.0 * 1024.0 * 1024.0);
    println!(
        "{name}: {gib_per_second:.2} GiB/s ({BLOCK_BYTES} byte blocks, {BLOCKS} blocks x {ROUNDS} rounds)"
    );
}
