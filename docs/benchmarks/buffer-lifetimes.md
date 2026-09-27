# Bounded shuffle storage and reader buffer reuse

Measured on 2026-09-27 against `d09ecce`. No format or public record API changes.

## Changes and safety

1. The runtime shuffle pool compacts only the incoming records it retains, using
   one payload copy. Previously, summing slice lengths underestimated retained
   memory because a small Bytes slice kept its entire source block alive.
   Immediately emitted records are not copied. Restore compacts arbitrary public
   checkpoint payloads too; selection, IDs, flags, payloads and RNG are unchanged.
2. Sync and async readers each retain only the latest stored-body allocation and
   latest decompressed allocation. An allocation is reused only if Bytes reports
   exclusive ownership. Outstanding records, slices and Python memoryviews remain
   immutable and prevent reuse; they never block reading the next block.
3. Each slot admits at most 64 MiB of **capacity**, not length. Larger jumbo
   allocations are not retained; EOF, failure or reader drop releases both slots.
   This is not a growing free-list or a cross-reader global pool. Capacity reuse
   avoids repeatedly zeroing already initialized stored-body memory. Decompression
   still validates the expected output size.

The shuffle byte limit covers its retained payload allocations, not record
metadata, upstream buffers, downstream consumers or separately held checkpoints.
The reader's two retention slots account for at most 128 MiB of allocation
capacity per active reader, excluding external records and in-flight allocations.
Small checkpoints/views may deliberately extend lifetime; reuse never overrides
ownership. Reader limits and integrity/authentication checks remain in force.

Tests cover release of tracked source blocks, externally constructed checkpoint
slices, exact checkpoint continuation, jumbo non-retention, pointer-level reuse,
and retained records across multiple blocks and reader destruction. Reader tests
cover None/Zstd, plaintext/encrypted and sync/async combinations. Python tests
retain derived readonly memoryviews through iteration and loader destruction,
with/without shuffle and None/Zstd. No unsafe code was introduced.

## Rust microbenchmarks

Intel Core Ultra 9 285K, rustc 1.98.0, release thin LTO, default target flags,
CPU 0 affinity, frequency not locked. Identical harnesses in both builds. Five
alternating pairs (AB, BA, AB, BA, AB), medians below.

| Reader case | Before GiB/s | After GiB/s | Change |
|---|---:|---:|---:|
| None | 5.383 | 5.930 | +10.2% |
| Zstd L1 | 33.169 | 33.074 | -0.3% |
| AES-256-GCM + None | 3.495 | 3.727 | +6.6% |

`reader_reuse` reads 128 MiB of synthetic 64 KiB records through approximately
16 MiB blocks, warms up then measures at least 500 ms per case. Inputs mostly
repeat byte 42 with an 8-byte record ID: **extremely compressible**, not a text
corpus or COCO. Logical GiB/s includes in-memory parsing, CRC, record iteration,
decryption where applicable, and decompression, but not I/O, whole-object hashing,
logical content fingerprinting, image decode or training. None and encrypted
ranges do not overlap between builds; Zstd ranges overlap substantially.

`shuffle_memory` streams 128 independently allocated 4 MiB blocks, split into
64 KiB records, through a 1,024-record pool with a nonbinding 256 MiB byte limit.
Both retain exactly 64 MiB of logical payload. Old code pins **316 MiB of source
blocks** at peak after dropping each producer block handle. New code pins **zero
source blocks at those boundaries**, owning 64 MiB of compact payload instead
(plus the current 4 MiB input block while processing). Source ownership counters
measure actual live allocations, not allocator arena retention or process RSS.
Do not add old logical payload bytes to old pinned bytes: those overlap.

Synthetic source allocation/fill + shuffle + drain throughput is 2.200 → 7.158
GiB/s. Extra copying does not make this particular workload slower; releasing
large source allocations promptly changes allocator/cache behavior. This is not
a promise of a 3.25x gain for real training. Small-record allocator overhead and
large-record copy costs remain workload-dependent.

[Raw Rust observations](buffer-lifetimes.csv) include all five pairs.

## Real COCO Python loader

Repacked all 5,000 COCO val2017 images using the current None format. Payloads
contain length-prefixed JSON annotations and original JPEG bytes: 834,534,171
payload bytes, including 814,705,164 JPEG bytes. Existing older-format artifacts
were not modified. Paired runs read the same newly built shard/layout.

Release native extensions, Python 3.14, CPU affinity 0–7, three alternating pairs
(AB, BA, AB). Each mode/version uses a fresh process. Source is a warm tmpfs local
filesystem backend, no LocalCache, network or GPU; native prefetch concurrency 2,
64 MiB budget. Shuffle uses 1,024 records / 256 MiB / seed 7.

| Metric (median) | Before | After |
|---|---:|---:|
| Record stream, records/s | 7,983 | 10,431 |
| Record stream, process peak RSS | 877 MiB | 281 MiB |
| Record stream, CPU seconds | 1.117 | 0.843 |
| Record stream, first record | 136 ms | 158 ms |
| Threaded JPEG/resize/collate, images/s | 2,838 | 2,786 |
| Threaded loader, process peak RSS | 1,452 MiB | 930 MiB |
| Threaded loader, CPU seconds | 7.651 | 7.565 |
| Threaded loader, first batch | 190 ms | 208 ms |

The threaded path has four decode workers, task size 8, decode prefetch 128,
batch size 64 and two prefetched batches. OpenCV/Torch internal CPU threads are
set to one in the benchmark process. It decodes JPEG, resizes to 224x224, produces
contiguous CHW uint8 tensors and collates; no augmentation, pinning or H2D.

**Tradeoff:** record-source throughput improves ~31% and its peak RSS falls ~68%.
The threaded path saves ~522 MiB RSS, but throughput is ~1.8% lower and first batch
is ~19 ms later. Compaction copies while initially filling the shuffle pool; this
is a memory-bound correction, not a guaranteed end-to-end speedup. Decode/collate
dominates this loader case. Three pairs are limited evidence, not a universal
performance claim. RSS includes libraries, native queues and tensor allocations.

[Raw COCO observations](buffer-lifetimes-coco.jsonl) preserve per-process metrics.

## Reproduce

Build the same harness sources/bench entries in each checkout:

```bash
cargo bench -p shardpack-format --bench reader_reuse --no-run --locked
cargo bench -p shardpack-runtime --bench shuffle_memory --no-run --locked
taskset -c 0 /path/to/reader_reuse
taskset -c 0 /path/to/shuffle_memory
```

Repack with `python/benchmarks/coco_writer.py --output /path/to/new/local/store`.
Then use its printed layout key (run from `python/` with the benchmark dependencies):

```bash
taskset -c 0-7 uv run --no-sync python benchmarks/coco_memory.py \
  --root /path/to/new/local/store --layout layouts/...layout.pb --mode records
taskset -c 0-7 uv run --no-sync python benchmarks/coco_memory.py \
  --root /path/to/new/local/store --layout layouts/...layout.pb --mode threaded
```

Use `--native /path/to/baseline/lib_native.so` to compare a separate release build
without replacing the installed extension. Alternate order and start a fresh
process per observation. `--cache` optionally exercises LocalCache, but was not
used in the reported COCO runs. Cold S3/CDN behavior is not measured here.
