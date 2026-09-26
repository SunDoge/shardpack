# zrip 0.8.10 versus 0.8.9

Measured on 2026-09-27. On these inputs, **ShardPack read throughput is essentially
unchanged**. Reused-output decoding changed by less than 2%. This measurement
does not demonstrate a meaningful training-throughput improvement or regression.

## Method

- Old: ShardPack commit `951b134`, zrip 0.8.9 / zrip-decode 0.8.8.
- New: ShardPack commit `dc4d57b`, zrip 0.8.10 / zrip-decode 0.8.9.
- zrip-core 0.11.0 and zrip-encode 0.9.0 are unchanged.
- Identical benchmark and ShardPack source, with each checkout's locked dependencies.
- Intel Core Ultra 9 285K, Linux x86_64, rustc 1.98.0 / LLVM 22.1.8,
  release thin LTO, default target flags, pinned to CPU 0; frequency not locked.
- Five alternating pairs (AB, BA, AB, BA, AB), warmup followed by at least
  200 ms per operation. Tables report medians in logical uncompressed GiB/s.
- Same 16 MiB inputs and harness as the [previous comparison](zrip-0.8.9.md):
  synthetic structured bytes, deterministic pseudorandom bytes, and sorted
  COCO val2017 JPEGs concatenated and truncated to 16 MiB.
- Decoder tests use identical C-zstd reference frames and a reusable decoder
  context. Reused and freshly allocated output buffers are measured separately.
  Decoded payloads are compared outside timing.
- ShardPack tests use each version's own encoded shard, a 16 MiB block target,
  64 KiB records and `without_content_fingerprint()`. Timing includes parsing,
  CRC, decompression, allocation and record iteration, but **not disk/network,
  full-object hashing, decryption, image decoding, augmentation or training**.

## Results

| Input | Level | Operation | 0.8.9 GiB/s | 0.8.10 GiB/s | Change |
|---|---:|---|---:|---:|---:|
| Structured | 1 | Encode | 2.747 | 2.765 | +0.7% |
| Structured | 1 | Decode, reused output | 8.170 | 8.218 | +0.6% |
| Structured | 1 | Decode, fresh output | 4.135 | 4.032 | -2.5% |
| Structured | 1 | ShardPack read | 6.997 | 7.080 | +1.2% |
| Structured | 3 | Encode | 2.238 | 2.262 | +1.1% |
| Structured | 3 | Decode, reused output | 7.739 | 7.797 | +0.7% |
| Structured | 3 | Decode, fresh output | 4.306 | 4.616 | +7.2% |
| Structured | 3 | ShardPack read | 5.074 | 5.069 | -0.1% |
| Random | 1 | Encode | 4.549 | 5.037 | +10.7% |
| Random | 1 | Decode, reused output | 22.747 | 22.840 | +0.4% |
| Random | 1 | Decode, fresh output | 2.475 | 2.123 | -14.2% |
| Random | 1 | ShardPack read | 0.993 | 1.003 | +1.0% |
| Random | 3 | Encode | 4.315 | 4.915 | +13.9% |
| Random | 3 | Decode, reused output | 22.487 | 22.383 | -0.5% |
| Random | 3 | Decode, fresh output | 2.131 | 2.006 | -5.9% |
| Random | 3 | ShardPack read | 4.280 | 4.244 | -0.8% |
| COCO JPEG | 1 | Encode | 1.825 | 1.841 | +0.9% |
| COCO JPEG | 1 | Decode, reused output | 18.921 | 19.262 | +1.8% |
| COCO JPEG | 1 | Decode, fresh output | 2.060 | 2.056 | -0.2% |
| COCO JPEG | 1 | ShardPack read | 3.993 | 3.994 | +0.0% |
| COCO JPEG | 3 | Encode | 1.827 | 1.795 | -1.8% |
| COCO JPEG | 3 | Decode, reused output | 15.875 | 16.162 | +1.8% |
| COCO JPEG | 3 | Decode, fresh output | 1.995 | 2.029 | +1.7% |
| COCO JPEG | 3 | ShardPack read | 3.929 | 3.927 | -0.1% |

## Interpretation and limits

- ShardPack read median changes range from -0.8% to +1.2%; all observed old/new
  ranges overlap. COCO reads are approximately 4 GiB/s in this in-memory test.
- Reused-output decoding changes range from -0.5% to +1.8%, with overlapping
  ranges throughout. No clear decoder speedup is established.
- All measured encoded sizes are unchanged. Structured input at level 1 is
  637,045 bytes and at level 3 is 484,922 bytes. This does not establish
  byte-for-byte output identity.
- Random encode medians improved 10.7% / 13.9%, but ranges overlap and the encoder
  dependency is unchanged. Allocation, cache history and executable layout can
  affect these measurements; do not attribute this to an encoder improvement.
- Fresh-output decoding remains noisy. Random L1 is 2.13–3.43 versus 1.99–2.15
  GiB/s; random L3 is 2.07–3.60 versus 1.99–2.08 GiB/s. Median declines alone
  are insufficient to establish a decoder regression.
- The large random ShardPack L1/L3 difference persists in both versions despite
  equal stored sizes. This is a harness/allocation-history limitation, not a
  compression-level recommendation.
- The reader allocates a fresh output buffer per block; reused-output results
  are not equivalent to the actual record-stream path. JPEG normally uses
  `BlockCodec::None`, so these zstd cases do not measure that recommended path.
- Keep the previous report as a historical measurement. Comparing absolute
  speeds across separate sessions is less reliable than this paired comparison.

## Reproduce

Build `crates/shardpack-format/benches/zrip_upgrade.rs` in each checkout:

```bash
cargo bench -p shardpack-format --bench zrip_upgrade --no-run --locked
```

Then pass the two executables to:

```bash
python3 scripts/compare-zrip.py \
  --old /path/to/0.8.9/benchmark --new /path/to/0.8.10/benchmark \
  --jpeg-dir /data/datasets/coco2017/val2017 --cpu 0 --pairs 5 \
  --output /tmp/zrip-0.8.10.csv
```

[Raw measurements](zrip-0.8.10.csv) contain all 240 observations.
