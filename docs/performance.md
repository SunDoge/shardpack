# Performance

ShardPack keeps executable microbenchmarks in the repository. Results below
were measured on the development x86-64 host and are directional rather than a
cross-machine guarantee.

## Format hot path

Run:

```bash
cargo bench -p shardpack-format --bench format_throughput
```

For 128 MiB of 256 KiB records packed into 16 MiB blocks, removing whole-block
temporary copies raised `none` write throughput from about 0.38 to 0.95 GiB/s
and full-validation read throughput from about 1.45 to 2.80 GiB/s in the latest
run. Training reads skip the redundant order-independent content fingerprint
while retaining CRC32C and exact shard BLAKE3 validation, reaching 5.32 GiB/s;
the encrypted training path reached 3.40 GiB/s.

The optimized path:

- computes directory and payload CRC32C separately with `crc32c-fast`, then
  combines the results without concatenating the buffers;
- transfers `None` payload buffers directly into `Bytes` without another copy;
- reuses writer directory and payload capacity between blocks;
- accounts for directory bytes before buffering a record, preventing tiny-record
  workloads from exceeding the configured block and shard soft targets;
- reserves the decoded record queue from the validated block directory instead
  of repeatedly growing it for blocks containing many small records;
- retains content fingerprint generation for materialization, import, and
  rebalance validation, but not for every training epoch.

The runtime shares one `Arc<LayoutManifest>` across a dataset and its loaders.
Shard assignment, prefetch, and active-reader state store indices into that
manifest rather than cloning `ShardMeta` values; retry recovery also reuses the
same manifest allocation. Distributed and node-affinity assignment sort those
indices directly, avoiding a second ID vector and an ID-to-index hash table per
loader.

The bounded shuffle pool performs O(1) random eviction when its record-count
limit binds and allocation-free reservoir selection when its byte limit binds.
The transitional Python reader is record-at-a-time but exposes each Rust `Bytes`
payload as a zero-copy read-only memoryview. The production Rust dataloader can
still avoid the remaining per-record Python object and scheduling overhead by
driving decode, batching, and checkpointing natively.

## Fory and PyTorch DataLoader

Run the reproducible CPU benchmark with:

```bash
mise run bench:python-fory
```

The benchmark uses 100,000 synthetic structured records averaging 129.9 encoded
bytes, Fory 1.7.3, Zstd level 1, a local filesystem store/cache, Python 3.12, and
PyTorch 2.14 CPU. A development-host run measured:

| Stage | Throughput |
| --- | ---: |
| Fory encode | 1.42–1.46 M records/s, 176–181 MiB/s |
| ShardPack writer | 2.22–2.40 M records/s, 274–297 MiB/s input |
| ShardPack memoryview + Fory decode | 0.70–0.75 M records/s |
| DataLoader, `num_workers=0` | 0.40 M records/s |
| DataLoader, one persistent spawned worker | 0.11 M records/s |
| DataLoader, 2–4 persistent spawned workers | 0.09–0.11 M records/s |

The 12.4 MiB encoded input occupied approximately 2.7 MiB after block
compression. These numbers are workload- and host-specific, not release claims.
They show that for tiny structured records the bottleneck is PyTorch's
per-sample Python IPC and default dictionary collation—not ShardPack I/O or Fory.
Use `num_workers=0` for cheap metadata-only decode. Multiple workers become useful
when image/audio transforms are expensive enough to amortize IPC. The dedicated
Rust dataloader should eventually batch before crossing into Python.

## Zstd implementation

Run:

```bash
cargo bench -p shardpack-format --bench compression_compare
```

The writer uses `zrip` with a reusable compression context and emits standard
zstd frames. The reader uses zrip's reusable decoder workspace and decompresses
directly into a caller-owned, exactly-sized `Vec`, which is then moved into
`Bytes` without copying. The dependency is temporarily pinned to the
`perf/decompress-into-caller-buffer` branch until this API is released.

Local measurements also showed that zrip encoding can outperform C zstd on
incompressible input and produce smaller frames for the structured-data sample
at level 1. The default level is 1 because ShardPack optimizes transfer
throughput rather than archival compression ratio.

The zstd frame checksum is disabled because every stored block already has a
CRC32C, encrypted blocks additionally have an AEAD tag, and full shard reads
validate the exact BLAKE3 identity.

## CDN cache sizing

The generic 1 GiB shard target is efficient for direct S3 streaming but exceeds
Cloudflare's 512 MB cacheable-object limit on non-Enterprise plans. A deployment
through Cloudflare and Backblaze B2 should use the 384 MiB target / 480 MiB hard
ceiling profile documented in [Cloudflare and Backblaze B2 deployment](cloudflare-b2.md).
This preserves complete-object caching and Cloudflare-managed Range responses;
local NVMe remains the reliable repeated-epoch cache.

## Python COCO ingestion

The release-mode Python buffer writer was tested against local COCO 2017. Each
record contained compact per-image annotations and the complete JPEG, so the
result was self-contained rather than an object URI index. Source, staging, and
output were on the same NVMe filesystem.

| Input | Records | Payload | Producer | End-to-end write | Sequential verify |
| --- | ---: | ---: | ---: | ---: | ---: |
| val2017, serial reader | 5,000 | 0.78 GiB | 0.984 GiB/s | 0.911 GiB/s | 2.259 GiB/s |
| train2017, serial reader | 118,287 | 18.42 GiB | 0.391 GiB/s | 0.391 GiB/s | 2.261 GiB/s |
| train2017, 8 readers / 32 images per task | 118,287 | 18.42 GiB | 0.824 GiB/s | 0.820 GiB/s | — |

The optimized full-train run reaches roughly 86% of the approximately 0.95
GiB/s native uncompressed writer result on this host. The remaining gap is in
Python small-file and record assembly work: 16 reader threads and 128 MiB batches
both regressed. A 64 MiB packed batch, eight file readers, and coarse 32-image
read tasks were the best tested settings.

Staging placement materially changes the result. Staging under `/tmp` while the
filesystem store was under `/data` forced a streamed cross-filesystem publish and
destination verification, reducing the small validation run to 0.125 GiB/s.
For a local store, staging should be on the same filesystem so publication can
use an atomic rename.

## Real dataset packaging smoke test

The ignored OCR integration test packages each image and its annotation into one
self-contained record, materializes multiple local shards, and then reads every
record back through the training runtime with byte-for-byte validation:

```bash
SHARDPACK_TEST_OCR_DATASET=/data/datasets/ocr_det_dataset_examples \
  cargo test -p shardpack-cli --test real_dataset -- --ignored --nocapture
```

The source directory is read-only and the generated layout is placed in temporary
directories that are removed after the test.

For a larger multi-shard run, the generic image-directory test expects `train/`
and `val/` trees and should be run in release mode:

```bash
SHARDPACK_TEST_IMAGE_DATASET=/data/datasets/imagenette2 \
  cargo test -p shardpack-cli --release --test real_dataset \
  packs_real_image_dataset_and_reports_sequential_throughput \
  -- --ignored --nocapture
```

On the development host, 13,394 Imagenette JPEGs produced six 256 MiB-target
shards containing 1,496.9 MiB. The original filesystem path copied each staging
file through OpenDAL and reached 104 MiB/s. Consuming the completed temporary
file with atomic no-clobber publication raised materialization to 717.5 MiB/s;
removing the subsequent redundant reread for that same-inode path raised a warm
run to 1,123.5 MiB/s. The source-compared and runtime-only reads in that run
reached 975.9 MiB/s and 1,214.2 MiB/s. The published object is identified by the
BLAKE3 accumulated over the exact bytes written. A same-filesystem atomic
publication preserves that inode. Streamed S3/cross-filesystem publications and
pre-existing destination objects are still fully read back and verified before
the layout is published. A cold-source run reached 259.3 MiB/s, illustrating that
opening 13k uncached JPEGs can dominate the packaging path.

After moving full retry-checkpoint construction off the per-record path and
returning already-decoded block records through a no-I/O fast path, a subsequent
warm run reached 1,680.8 MiB/s for the runtime-only payload pass (1,109.1 MiB/s
materialization and 1,087.4 MiB/s source-compared read in the same run). These
figures remain host-cache-sensitive and are regression evidence, not guarantees.

The same test covers full COCO 2017 by selecting its directory names:

```bash
SHARDPACK_TEST_IMAGE_DATASET=/data/datasets/coco2017 \
SHARDPACK_TEST_IMAGE_TREES=train2017,val2017 \
  cargo test -p shardpack-cli --release --test real_dataset \
  packs_real_image_dataset_and_reports_sequential_throughput \
  -- --ignored --nocapture
```

COCO's 123,287 JPEGs produced 76 shards containing 19,203.6 MiB. A first run
with cold source data materialized at 226.6 MiB/s and performed byte-for-byte
source comparison at 522.8 MiB/s. A warm-cache run reached 797.2 MiB/s,
1,129.6 MiB/s with source comparison, and 1,427.4 MiB/s for a runtime-only
payload pass. The cold/warm gap reflects opening 123k source files; sequential
ShardPack reads avoid that small-file I/O pattern.

The compression analyzer was also run over a self-contained, uncompressed
COCO `val2017` layout. Its 5,000 records contained 814,705,164 JPEG bytes and
834,534,171 total payload bytes, so JSON annotations and record framing were
about 2.4% of the payload. Repacking the sample into 51 16 MiB blocks produced
the following estimate:

```text
none_estimated_bytes=834657499
zstd_estimated_bytes=834355893
zstd_ratio=0.999639
savings_percent=0.036
recommendation=none threshold_percent=10.000
```

Zstd saved only 301,606 bytes (about 295 KiB). Compressible annotations did not
offset the cost and negligible size benefit of passing the dominant JPEG data
through block compression. This supports `None` as the default for media-heavy
and ordinary image-plus-metadata layouts.

### COCO PyTorch reader

The self-contained COCO val2017 layout was also read through the real PyTorch
adapter. The decode function parsed the compact metadata prefix, decoded each
JPEG with OpenCV, resized it to 224 by 224, returned a CHW CPU tensor, and used
64-image batches. The local verified cache was hot after the first pass.

| DataLoader workers | Pool shuffle | Throughput |
| ---: | ---: | ---: |
| 0 | disabled | 225 images/s |
| 1 | disabled | 655 images/s |
| 2 | disabled | 655 images/s |
| 4 | disabled | 646 images/s |
| 1 | 4,096 records / 256 MiB | 636 images/s |

This validation layout contains one approximately 796 MiB shard. Since workers
are assigned whole shards, only one worker can consume it; additional workers
correctly remain idle and provide no throughput benefit. Pool shuffle reduced
throughput by about 3% while retaining byte-bounded memory use. These results
support starting with one DataLoader worker per training rank and increasing it
only when the rank owns multiple shards and profiling shows GPU input stalls.
They are host-specific measurements, not release claims.

A subsequent single-process threaded-loader run capped the nested OpenCV and
Torch native pools at one thread and used ordered eight-record decode tasks:

| Loader | Decode workers | Throughput |
| --- | ---: | ---: |
| PyTorch DataLoader process | 1 | 464 images/s |
| ShardPack threaded | 1 | 481 images/s |
| ShardPack threaded | 2 | 842 images/s |
| ShardPack threaded | 4 | 1,406 images/s |
| ShardPack threaded | 8 | 1,420 images/s |

Four decode threads were sufficient on this host and were approximately three
times as fast as the same-run PyTorch process loader. Eight threads added memory
without material throughput. The batch handoff uses the standard bounded
`queue.Queue`; at roughly 22 batches/s its synchronization is not a meaningful
cost. Decode task/Future scheduling was reduced by processing small chunks.

Reproduce the reader measurement with:

```bash
python python/benchmarks/coco_dataloader.py \
  --config /path/to/shardpack.toml \
  --layout layouts/<layout-id>.layout.pb \
  --cache /local/nvme/shardpack-cache \
  --workers 0 1 2 4
```

As a text-only comparison, the analyzer was run on the official Salesforce
WikiText-2 raw training split. Each non-empty text row became one opaque record:

```text
sample_records=23767
sample_payload_bytes=10914845
sample_blocks=1
none_estimated_bytes=11485381
zstd_estimated_bytes=5175808
zstd_ratio=0.450643
savings_percent=54.936
recommendation=zstd threshold_percent=10.000
```

Writing both layouts confirmed the estimates exactly: the uncompressed shard
was 11,485,381 bytes and the Zstd level 1 shard was 5,175,808 bytes. Unlike the
COCO result, this workload clearly justifies block compression.

## RustFS / S3-compatible storage

The retained loopback benchmark transfers a 256 MiB object:

```bash
SHARDPACK_TEST_S3_BUCKET=shardpack-integration \
SHARDPACK_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=rustfsadmin \
AWS_SECRET_ACCESS_KEY=rustfsadmin \
cargo test -p shardpack-storage --test s3_throughput --release \
  -- --ignored --nocapture
```

Against `/data/rustfs`, upload throughput rose from about 111 MiB/s with the
serial OpenDAL writer to about 678 MiB/s with eight concurrent 16 MiB multipart
parts. Sixteen parts reached about 701 MiB/s but doubled bounded buffering for a
small gain, so the default remains eight parts (roughly 128 MiB per active
object upload).

Use the RustFS benchmark for regression tracking, not as a prediction of WAN S3
performance. Provider limits, RTT, NIC bandwidth, TLS, and cross-region traffic
can dominate production results.

## Control-plane memory bounds

Snapshot and layout protobuf reads are streamed through a hard 512 MiB default
limit instead of using an unbounded object-store `read_all`. Rust callers with
exceptionally large layouts can select an explicit bound through
`load_snapshot_with_limit` and `load_layout_with_limit`. Catalog messages use a
64 MiB bound. Exact-limit objects are accepted; the reader consumes at most one
additional byte before returning `ReadLimitExceeded` for oversized objects.

## Local-cache coordination

Cold misses are coordinated with bounded, striped cross-process file locks, so
multiple Python workers requesting the same shard perform one object-store
download. Lock waiting is asynchronous and does not occupy a Tokio blocking
thread. Cache capacity remains a soft limit: the exact on-disk inventory is
scanned under a cross-process mutation lock, but the LRU candidate sort is
skipped while the inventory is below the configured limit. Each shard hit makes
one best-effort mtime update, so eviction reflects access recency rather than
download age without adding metadata writes to the record path. A stale
corruption report rechecks the current path under the shard download lock before
deletion, so a slow worker cannot remove a valid copy that another worker already
repaired.
