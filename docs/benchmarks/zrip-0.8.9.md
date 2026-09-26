# zrip 0.8.9 versus the caller-buffer patch

Measured on 2026-09-26. The release is not uniformly faster. Reused-output decoding
improved modestly on this machine, while this synthetic structured input regressed
in encoding and ShardPack reading, particularly at level 1.

## Method

- Old: zrip 0.8.8 fork at `bfde6ef7c5f81692a571518467e125c56e840968`.
- New: crates.io zrip 0.8.9, with its locked transitive dependencies.
- Identical ShardPack implementation and benchmark source in two builds; old
  dependency graph from ShardPack commit `cf19a90`, new from `8540313`.
- Intel Core Ultra 9 285K, Linux x86_64, rustc 1.98.0 / LLVM 22.1.8,
  release with thin LTO, identical default target flags, pinned to CPU 0.
- Five alternating old/new pairs (AB, BA, AB, BA, AB). Each operation warms up
  and then runs for at least 200 ms per invocation. Tables show median logical
  uncompressed GiB/s; higher is better. CPU frequency was not locked.
- 16 MiB inputs: synthetic structured JSON-like bytes with deterministic changes,
  deterministic pseudorandom bytes, and sorted COCO val2017 JPEG bytes concatenated
  and truncated to 16 MiB. Structured data is not a natural-language corpus.
- Compression uses a reusable context, with content checksum disabled as in the
  writer. Decoder comparisons use identical C-zstd-generated frames for
  both versions (Rust `zstd` crate 0.13.3, C zstd 1.5.7), and a reusable decoder
  context. Buffer-reuse and fresh-allocation
  measurements are separate. Full payload comparison is outside timed regions.
- ShardPack read uses each version's own encoded shard, a 16 MiB block target,
  64 KiB records, and `without_content_fingerprint()`. It includes block parsing,
  CRC, decompression, allocations and record iteration, but excludes disk/network,
  full-object hashing, decryption, image decoding and model training.

## Results

| Input | Level | Operation | Old GiB/s | New GiB/s | Change |
|---|---:|---|---:|---:|---:|
| Structured | 1 | Encode | 4.840 | 2.671 | -44.8% |
| Structured | 1 | Decode, reused output | 7.734 | 8.140 | +5.2% |
| Structured | 1 | Decode, fresh output | 4.061 | 3.504 | -13.7% |
| Structured | 1 | ShardPack training read | 8.333 | 6.996 | -16.0% |
| Structured | 3 | Encode | 2.519 | 2.269 | -9.9% |
| Structured | 3 | Decode, reused output | 7.290 | 7.745 | +6.2% |
| Structured | 3 | Decode, fresh output | 3.115 | 1.917 | -38.5% |
| Structured | 3 | ShardPack training read | 5.400 | 5.067 | -6.2% |
| Random | 1 | Encode | 1.855 | 2.064 | +11.2% |
| Random | 1 | Decode, reused output | 21.108 | 22.085 | +4.6% |
| Random | 1 | Decode, fresh output | 1.933 | 2.108 | +9.1% |
| Random | 1 | ShardPack training read | 0.955 | 0.935 | -2.0% |
| Random | 3 | Encode | 1.848 | 1.957 | +5.9% |
| Random | 3 | Decode, reused output | 20.943 | 22.141 | +5.7% |
| Random | 3 | Decode, fresh output | 2.052 | 2.046 | -0.3% |
| Random | 3 | ShardPack training read | 4.343 | 4.394 | +1.2% |
| COCO JPEG | 1 | Encode | 1.877 | 1.819 | -3.1% |
| COCO JPEG | 1 | Decode, reused output | 18.941 | 19.363 | +2.2% |
| COCO JPEG | 1 | Decode, fresh output | 2.012 | 1.953 | -2.9% |
| COCO JPEG | 1 | ShardPack training read | 4.353 | 3.975 | -8.7% |
| COCO JPEG | 3 | Encode | 1.847 | 1.818 | -1.6% |
| COCO JPEG | 3 | Decode, reused output | 14.715 | 16.570 | +12.6% |
| COCO JPEG | 3 | Decode, fresh output | 1.964 | 2.030 | +3.3% |
| COCO JPEG | 3 | ShardPack training read | 4.394 | 4.062 | -7.5% |

Structured level-1 encoding produced **437,535 → 637,045 bytes (+45.6%)**.
Level 3 produced **525,218 → 484,922 bytes (-7.7%)**. The codec remains zstd
compatible; encoded byte identity and compression ratio are not stable across
encoder versions. COCO and random input remained approximately their raw size.

The structured encode and ShardPack-read regressions had non-overlapping observed
ranges across these five runs. Fresh-output decoding varied substantially (e.g.
structured L1 old 2.50–4.52 versus new 2.31–4.60 GiB/s), so those median changes
should not be treated as established decoder regressions. Most small changes on
random/JPEG data overlap run-to-run variation.

The random ShardPack L1/L3 read difference occurs in both builds despite equal
stored sizes. This harness is sensitive to allocation/cache history; do not use
that difference to choose compression level. The paired version comparison and
raw results are more informative than absolute throughput across test cases.

For our reader, caller-owned output currently means a **new Vec per block**, not
reusing a Vec still held by records. Thus the reused-output microbenchmark gain
does not automatically translate into the training hot path. JPEG normally uses
`BlockCodec::None`, so the JPEG zstd results do not imply a regression in the
recommended uncompressed image-container path. No rollback was performed.

## Reproduce

Benchmark: `crates/shardpack-format/benches/zrip_upgrade.rs`.
Build it in each dependency checkout using the identical source and bench entry:

```bash
cargo bench -p shardpack-format --bench zrip_upgrade --no-run --locked
```

Pass the two resulting executable paths to:

```bash
python3 scripts/compare-zrip.py \
  --old /path/to/old/benchmark --new /path/to/new/benchmark \
  --jpeg-dir /data/datasets/coco2017/val2017 --cpu 0 --pairs 5 \
  --output /tmp/zrip-comparison.csv
```

[Raw measurements](zrip-0.8.9.csv) include each run's speed and stored byte count.
