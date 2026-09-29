# Full COCO train2017: encrypted S3 and plaintext cache

2026-09-30. Follow-up to the [8,192-image S3 experiment](coco-internal-s3.md),
using **all 118,287 train2017 images** and all 5,000 val2017 images. Training
shards are encrypted on internal S3; validation remains local. One seed (42),
RTX 5080, pretrained YOLO11n, 640 resolution, batch 16, FP16 AMP, four decode
threads, three epochs. Same unaugmented benchmark recipe, not the standard
fully augmented YOLO recipe.

## Results

| Metric | Epoch 1: empty cache | Epoch 2 | Epoch 3 |
| --- | ---: | ---: | ---: |
| Training images/s | 179.66 | 537.77 | 532.83 |
| Training seconds | 658.408 | 219.958 | 221.998 |
| First batch from epoch start (s) | 17.367 | 0.429 | 0.432 |
| Cumulative batch waiting (s) | 417.371 | 0.427 | 0.428 |
| Cache hits / misses | 0 / 145 | 145 / 0 | 145 / 0 |
| Downloaded shard bytes | 19,392,100,756 | 0 | 0 |

Every epoch read **118,287 records / 7,393 batches / 145 shards**. The final
batch contains seven images. AMP remained enabled and batch size did not change.
Retries, checksum failures, corrupt cache entries and cache evictions were zero.

The first batch was delivered after downloading 216,118,621 bytes, **1.11%** of
the remote shards, with one shard complete. Training therefore overlapped cache
construction instead of waiting for the entire dataset. Warm throughput averages
535.30 images/s, about 2.98 times the cold epoch. Warm batch waiting is almost
entirely initial startup; data delivery is not the main observed warm bottleneck.
Pipeline timing categories overlap and must not be summed as exclusive costs.

Validation and best-checkpoint reload succeeded. Best mAP50–95 was **37.14%**;
total training invocation wall time was 1,145.29 s, including setup and validation,
but excluding offline packaging/upload. No failed training attempts were excluded.

## Preparation and configuration

- Local packaging included all images, including images without retained boxes.
  All payload bytes and SampleIds were checked against original COCO; the first
  128 records per split also had exact decoded image/label tensor comparisons.
- Python writer streamed the local train layout into AES-256-GCM S3 objects:
  128 MiB target shards, 4 MiB blocks, no block compression. Repack/upload took
  **885.97 s**, producing 145 shards / 19.39 GB (18.06 GiB).
- The benchmark now accepts `--cache-max-bytes`. The old 8 GiB budget would not
  hold this dataset; this run used **32 GiB**, with plaintext cache initially empty.
- Default network prefetch: two streams / 64 MiB. Encoded shuffle pool:
  1,024 records / 256 MiB. No tuning between epochs.
- Endpoint and credential settings came from the existing private TOML and
  `uv --env-file`; no credentials or encryption key bytes are recorded here.
- The original idle application held about 1 GiB GPU memory. GPU checks showed
  no other competing compute job, but this was not an exclusive-host benchmark.

## Interpretation and limits

This validates the intended full-training-set workflow: encrypted object storage
to streaming training plus a derived plaintext cache, then cache-only shard reads.
It does **not** establish a speedup over map-style loading: no paired full-dataset
map-style baseline was run. Nor does the short single-seed run establish accuracy
equivalence; use the separate [alignment experiment](coco-yolo-alignment.md) for
the earlier, smaller paired comparison.

This host has approximately 126 GiB RAM; the 18.06 GiB training layout fits in
memory. Warm cache reads can benefit from OS page cache. This is **not** an
out-of-memory-dataset experiment or isolated NVMe benchmark. Origin caches were
not controlled. The cold epoch measures an empty local ShardPack cache, not a
cold storage server. No remote JPEG baseline, CDN, public bucket, B2 egress bill,
multi-node training or multi-seed convergence was tested.

Zero downloaded shard bytes does not imply zero HTTP requests: each iteration
still fetches layout metadata from S3. The direct-reader `shards_verified=0`
counter is not the cache verification counter; cache-layer identity and
authentication checks remain enabled, as in the preceding S3 experiment.

## Reproduce

With the `torch` and `yolo` uv groups installed:

```bash
cd python
uv run --no-sync python benchmarks/coco_yolo_alignment.py prepare \
  --work /data/datasets/new-full-coco-run --train-count 118287

uv run --no-sync --env-file /path/to/private.env python benchmarks/coco_s3_prepare.py \
  --work /data/datasets/new-full-coco-run \
  --config-path /path/to/private-s3.toml --store internal

# Use the source_spec printed above; its cache must be empty for a cold run.
uv run --no-sync --env-file /path/to/private.env python benchmarks/coco_yolo_alignment.py stream \
  --work /data/datasets/new-full-coco-run --model /path/to/yolo11n.pt \
  --epochs 3 --amp --run-name full-internal-s3-amp \
  --source-spec /path/to/source.json --cache-max-bytes 34359738368
```

The checkpoint SHA256 and exact options are in the [raw result](coco-full-internal-s3.json).

## Retained artifacts

Local experiment: `/data/datasets/shardpack-coco-full-s3-20260930`, approximately
38 GiB including plaintext source layouts, derived cache, logs and checkpoints.
Sibling `-prepare.log`, `-upload.log`, `-train.log` and `-tests.log` preserve logs.
The AMP self-check's downloaded YOLO26n weight is archived here; training used
the original YOLO11n checkpoint, not that self-check model.

Remote objects occupy 19.39 GB plus the manifest, retained under the isolated
suffix `experiments/coco-s3-2a01594518fc43e1a5472d9b03b5922a` beneath the existing
configured store root. No existing objects, bucket ACLs or policies were changed.
The local `coco-s3-2a01594518fc43e1a5472d9b03b5922a/source.json` records the source
descriptor and key-file path. The random 32-byte key is mode 0600 on `/dev/shm`:
it will be lost on reboot unless securely backed up. This is rebuildable benchmark
data, not a durable-only copy. No remote or local benchmark data was deleted.

Verification: 62 Python tests passed; changed Python files passed Ruff checks and
formatting. The only functional change is the benchmark cache-budget option and
its positive-value validation; no Rust/runtime implementation changed in this run.
