# Internal S3: encrypted streaming training and plaintext cache

2026-09-29, RTX 5080, YOLO11n FP16 AMP, batch 16, 640 images, four decode
threads, seed 42. Same 8,192 training / 5,000 validation images and three-epoch
recipe as the [AMP experiment](coco-yolo-amp.md). **Training shards are remote;
validation remains local.** No CDN/HTTP gateway was involved.

## Result

| Metric | Epoch 1: empty node cache | Epoch 2 | Epoch 3 |
| --- | ---: | ---: | ---: |
| Training images/s | 175.15 | 517.34 | 563.88 |
| Training seconds | 46.772 | 15.835 | 14.528 |
| First batch from epoch start (s) | 5.451 | 0.350 | 0.355 |
| Cumulative batch waiting (s) | 23.307 | 0.349 | 0.351 |
| Cache hits / misses | 0 / 10 | 10 / 0 | 10 / 0 |
| Downloaded shard bytes | 1,338,564,602 | 0 | 0 |

At the first batch, only **232,445,707 bytes (17.4%)** had been downloaded,
with one completed shard. Training therefore started while most remote data
was still outstanding; there was no full-dataset staging barrier.

All three epochs consumed 8,192 records / 512 batches and completed ten shards.
Retries, checksum failures, corrupt cache entries and evictions were zero. Warm
epochs hit the derived plaintext cache and downloaded no shard payload. Layout
metadata is still fetched from S3, so this does **not** mean zero network requests.
Cache-layer integrity checks remain enabled; the direct-reader `shards_verified`
counter is zero on this cache path and is not a cache verification counter.

Warm throughput averages about 541 images/s, close to the preceding local AMP
experiment's 533 images/s. These separate short runs do not establish a speed
advantage. Warm plaintext data also fits in the OS page cache. The cold epoch
includes network stalls, initial pool fill and model warmup, so 175 images/s is
not an isolated network bandwidth benchmark. Origin-side caching is uncontrolled.
Validation and best-checkpoint reload completed; best mAP50–95 was 34.028%.
Encrypted repacking changes physical shard IDs/order, so this is not a same-order
precision-equivalence test.

## Setup and retained artifacts

- Re-streamed the existing local training layout into AES-256-GCM objects using
  the Python writer: 128 MiB target shards, 4 MiB blocks, no block compression.
- Ten shards, 1,338,564,602 stored bytes; combined repack/upload took 77.04 s.
- Storage settings and credentials came from the existing private config and
  `uv --env-file`. Endpoint, bucket credentials and key bytes are not in reports.
- A unique suffix was appended below the configured store root:
  `experiments/coco-s3-8c7d66a22473449dbe0051f4acec5d3f`.
  Existing remote objects and bucket policy/ACL were not modified.
- Used `cache_mode="plaintext"`, 8 GiB cache budget, default network prefetch
  (two concurrent streams / 64 MiB), and 1,024-record / 256 MiB shuffle pool.
- The active experiment key is a raw 32-byte file under `/dev/shm`, created with
  mode 0600. Its path is recorded only in the local source descriptors. No key
  bytes are uploaded, committed, printed or placed in training arguments.

Remote objects are **retained**, occupying about 1.34 GB plus a manifest. Local
diagnostics and cache attempts occupy about 2.8 GiB under
`/data/datasets/shardpack-yolo-alignment-20260929/coco-s3-8c7d66a22473449dbe0051f4acec5d3f`.
The successful descriptor is `source-final.json`; its cache is
`plaintext-cache-final`. Treat this as temporary benchmark data: the tmpfs key
will be lost on reboot unless explicitly backed up securely. Original COCO and
previous layouts are unchanged. No remote cleanup was performed.

## Failures found and excluded

1. An initial prepare attempt lacked the credential environment variables and
   failed before upload. Loading the specified env file resolved it.
2. The first training attempt revealed that live `source.stats()` called a
   thread-affine PyO3 reader from the consumer thread. Fixed by exposing a frozen,
   shareable atomic-counter handle; the reader/runtime/key are not shared.
   Regression tests cover cross-thread polling during decode and handles outliving
   readers. The interrupted cache was retained and not used for the measured run.
3. An external image-generation job occupied about 12 GiB during a retry, causing
   OOM and upstream automatic batch reduction to eight. The benchmark rejected
   the changed batch/step count. It now also rejects batch changes at epoch start.
   The final run used another empty cache after the competing task exited.

Only the final run is reported above. Failed-run logs and caches remain locally.
During the final run, the existing idle application held about 1 GiB; no extra
GPU process was observed at checks. This was not an exclusive-host benchmark.

## Reproduce

Enable the `torch` and `yolo` uv groups, then prepare a new isolated prefix:

```bash
cd python
uv run --no-sync --env-file /path/to/private.env python benchmarks/coco_s3_prepare.py \
  --work /data/datasets/shardpack-yolo-alignment-20260929 \
  --config-path /path/to/private-s3.toml --store internal
```

The script prints a `source_spec` path without credentials. Pass that descriptor
and an unused run name; its newly created cache is initially empty:

```bash
uv run --no-sync --env-file /path/to/private.env python benchmarks/coco_yolo_alignment.py stream \
  --work /data/datasets/shardpack-yolo-alignment-20260929 \
  --model /data/datasets/shardpack-yolo-alignment-20260929/yolo11n.pt \
  --epochs 3 --amp --run-name internal-s3-repeat --source-spec /path/to/source.json
```

Reusing the same descriptor also reuses its cache; that is not a cold-cache rerun.
[Raw successful result](coco-internal-s3.json) includes first-batch and end-of-epoch
cache counters. The uploaded ciphertext is still private in this experiment;
it does not test B2/Cloudflare public distribution or egress billing.
