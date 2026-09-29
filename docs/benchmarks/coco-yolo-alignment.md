# COCO map-style vs ShardPack training alignment

This is a **single-seed, medium-scale fine-tuning experiment**, not a claim of
full-COCO convergence equivalence. Acceptance target: ShardPack's COCO-style
`mAP50–95` no more than **1 absolute percentage point** below the map-style arm.
The evaluator is Ultralytics' detection validator on the complete val2017 split,
not an independent pycocotools evaluation with official crowd handling.

## Result (2026-09-29)

RTX 5080 16 GiB, Torch 2.14.0+cu130, Ultralytics 8.4.159. Local objects on
`/data` NVMe; data fits in RAM and verification warms the OS page cache. This is
not a cold-NVMe throughput measurement. Both runs completed all ten epochs.

| Measurement | Map-style | ShardPack stream |
| --- | ---: | ---: |
| Best checkpoint mAP50–95 (%) | 31.526 | 31.289 |
| Last epoch mAP50–95 (%) | 31.508 | 31.232 |
| Median train images/s, epochs 2–10 | 343.81 | 344.08 |
| Training + validation + final evaluation wall time (s) | 380.03 | 382.16 |

Observed best-checkpoint gap: **−0.237 percentage points**; last-epoch gap:
**−0.276 points**. This pair meets the proposed one-point threshold, but does
not prove equivalence across seeds, full-data training or augmented training.

Speed is effectively tied (median ratio 1.0008). **Do not claim a speedup**:
the GPU was not exclusive. A pre-existing process held about 1 GiB throughout;
at 18:47:52 HKT an additional `qwen_image_2_1_generate` process occupied about
8.5 GiB during stream epoch 7. That epoch dropped to 228 images/s, and both arms
contain timing outliers. None of these epochs were discarded from the medians.
An uncontended, order-balanced repeated timing experiment is still needed.

The streaming loader delivered/verified all 8,192 training records and all ten
train shards each epoch, with zero retries/checksum failures. Observed cumulative
batch waiting was 0.14–0.36 seconds per epoch, mostly the initial batch, while
the producer usually waited for the trainer to consume queued batches. This is
evidence that the loader supplied this model fast enough, not a standalone
measurement of maximum reader throughput.

Raw artifacts: [summary](coco-yolo-alignment-summary.json),
[map arm](coco-yolo-alignment-map.json),
[stream arm including loader timings](coco-yolo-alignment-stream.json).
Large artifacts (packed objects, selected annotations, logs, best/last weights)
remain in `/data/datasets/shardpack-yolo-alignment-20260929`, about 2.1 GiB plus
checkpoints. Original COCO data is unchanged. The initial checkpoint is archived
as `yolo11n.pt` in that directory; SHA256:
`0ebbc80d4a7680d14987a577cd21342b65ecfd94632bd9a8da63ae6417644ee1`.

## Controlled comparison

- COCO train2017: 8,192 images selected after a deterministic Python shuffle,
  seed 42; val2017: all 5,000 images. No train/val overlap.
- YOLO11n, identical COCO-pretrained checkpoint, 640 square letterbox, batch 16,
  AdamW (`lr0=0.0001`, `warmup_bias_lr=0`, `nbs=16`), one warmup epoch,
  10 epochs, seed 42, FP32,
  deterministic Torch mode, no early stopping.
- Shared JPEG decoder and bbox preprocessing, no random augmentation, no Mosaic.
  Both arms use the same trainer/model/loss/validator/checkpoint-selection path.
- Map arm: custom map-style dataset over original JPEG files, PyTorch DataLoader,
  full random sampler, four persistent worker processes, two prefetched batches
  per worker, pinned memory.
- Stream arm: immutable `.spk` objects on local disk, Rust shard/pool shuffle
  (1,024 records / 256 MiB), four decode threads, pinned memory after collate.
  Offline packing order is the same shuffled selected image list.
- Validation has no shuffle in either arm. Annotations: exclude crowd, clip to
  image boundaries, remove invalid boxes and exact duplicate class/box pairs;
  sorted COCO category IDs map to the same 80 contiguous class IDs.

Map-vs-pool ordering is intentionally different. Equal seed does not imply
equal minibatches. This experiment does **not** compare to Ultralytics' default
map-style image resize/rect/Mosaic pipeline: preprocessing is shared to isolate
the loader and sample-ordering change. Pretraining has already seen COCO; this
is an integration/fine-tuning test, not independent generalization evidence.

## Reproduce

Requires the existing ShardPack Python environment plus Ultralytics 8.4.159,
OpenCV and CUDA Torch. `prepare` refuses to overwrite an existing experiment.

```bash
cd python
uv sync --group torch --group yolo
uv run --no-sync maturin develop --release
uv run --no-sync python benchmarks/coco_yolo_alignment.py prepare \
  --work /data/datasets/shardpack-yolo-alignment-20260929
uv run --no-sync python benchmarks/coco_yolo_alignment.py map \
  --work /data/datasets/shardpack-yolo-alignment-20260929 --model /path/to/yolo11n.pt
uv run --no-sync python benchmarks/coco_yolo_alignment.py stream \
  --work /data/datasets/shardpack-yolo-alignment-20260929 --model /path/to/yolo11n.pt
uv run --no-sync python benchmarks/coco_yolo_alignment.py compare \
  --work /data/datasets/shardpack-yolo-alignment-20260929
```

`experiment.json` contains selected image IDs, annotations, layout locations,
annotation/selection hashes and packing times. Both arms write per-epoch metrics
and timing JSON plus best/last checkpoints. Run arms in separate processes.
The prepare step compares **every packed payload** with original metadata/JPEG
bytes, checks exact ID coverage, and verifies identical decoded tensors for the
first 128 records of each split. Unit tests independently check letterbox
geometry, channel order and empty annotations.

Selection SHA256s for this run (serialized ordered image/annotation rows):

- Train: `0d758cec240540de032abeb77a3b7dd5ab71efa9f0843041a1e8642354bd15dd`
- Val: `7872a1be4e9e8317d79a26a08f99d7f5dfac67c02a4dd5cc5e660145c9523a41`

Training timing uses synchronized epoch boundaries and excludes validation.
First-epoch worker startup and cache warmup are included in that epoch; steady
state should be summarized separately. Total wall time includes validation and
final best-checkpoint evaluation. Parent RSS excludes map worker memory and
must not be used as a whole-pipeline memory comparison.

## Scope and remaining evidence

An initial map-arm diagnostic used AdamW `lr0=0.001` while leaving upstream's
`warmup_bias_lr=0.1` unchanged. It produced severe early metric degradation and
was interrupted before running the streaming arm. Its logs, partial checkpoints
and epoch metrics are retained under `map-warmup-diagnostic*` in the experiment
directory. The comparison was restarted with the shared fine-tuning parameters
above; the diagnostic is not one of the comparison arms. Changing both learning
rate settings is a protocol correction, not an isolated causal experiment on
which setting caused the degradation.

This local-storage test does not measure B2/CDN throughput, cold-cache economics,
distributed rank scheduling, Mosaic integration, or full-data convergence.
One seed cannot establish a robust non-inferiority bound. If the observed gap
is within one point, the next step is a larger training set and multiple seeds,
then matched stochastic augmentation including Mosaic. Speed is an independent
criterion: an acceptable metric gap does not prove a speedup.

Follow-up: [targeted cold-cache loader startup](coco-cold-loader.md) measures
loader-only cold/warm performance and verifies block-storage reads via Linux
process I/O counters.
