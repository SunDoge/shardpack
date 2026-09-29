# YOLO11n FP32 vs FP16 AMP with ShardPack

2026-09-29, RTX 5080 16 GiB, Torch 2.14.0+cu130, Ultralytics 8.4.159.
Same 8,192 train / 5,000 validation COCO selection, decoder, initial checkpoint,
seed 42, 640 resolution, batch 16, four decode threads, deterministic mode and
AdamW recipe as the [alignment experiment](coco-yolo-alignment.md).

Both arms were freshly run for **three epochs**, FP32 first then AMP, in separate
processes. This is a short throughput check, not a convergence study or an
order-balanced benchmark. A pre-existing process held about 1 GiB of GPU memory;
no additional GPU workload was observed during the checks. OS file cache is warm.

| Metric | FP32 | FP16 AMP |
| --- | ---: | ---: |
| Train images/s, epochs 2–3 median | 352.26 | 533.27 |
| Epoch 2 train time | 23.191 s | 15.614 s |
| Epoch 3 train time | 23.321 s | 15.117 s |
| Peak Torch allocated tensor memory | 3,988.74 MiB | 2,362.30 MiB |
| Epoch 2 cumulative batch wait | 0.181 s | 0.205 s |
| Epoch 3 cumulative batch wait | 0.146 s | 0.156 s |
| Best-checkpoint validation mAP50–95 (%) | 34.087 | 34.127 |
| Total wall time including setup/validation/final evaluation | 113.30 s | 98.38 s |

Steady training throughput increases **51.4%** (1.514×); peak allocated tensor
memory decreases **40.8%**. First-epoch warmup is excluded from the throughput
summary, but no later epochs are discarded. Memory is Torch allocated memory,
not total GPU/process memory reported by `nvidia-smi`.

The producer still supplies data fast enough: batch waiting remains around
0.16–0.20 seconds over a 15-second epoch, with most of that at initial pool fill.
The result supports enabling AMP for this workload; it does not demonstrate
that the GPU alone is the bottleneck or that further reader optimization would
improve training throughput.

AMP compatibility checks passed, and the benchmark asserts the trainer's actual
AMP state before every epoch. All 8,192 records and ten training shards were
verified each epoch, without checksum failures or retries. Training, validation
and best-checkpoint reload completed with finite reported metrics. Validation
uses each trainer's precision mode, so the small mAP difference is not a
controlled FP32-evaluation accuracy comparison. The three-epoch LR schedule
also differs from the prior ten-epoch experiment; do not compare their absolute
mAP values as evidence of a precision effect.

Ultralytics downloaded `yolo26n.pt` for its one-time AMP self-check only; both
training arms used the identical archived `yolo11n.pt`. Setup/check/download
cost is included in total wall time, not epoch throughput. The self-check weight
was archived as `amp-check-yolo26n.pt` in the experiment directory.

## Follow-up: map-style with AMP

The missing map-style arm was subsequently run with the same initial checkpoint,
seed, three-epoch schedule, batch size, image size and FP16 AMP setting. Actual
AMP state was asserted for every epoch; training, validation and best-checkpoint
reload completed. No additional active GPU process was observed at the checks.

| Metric | Map-style AMP | ShardPack AMP |
| --- | ---: | ---: |
| Epoch 2 images/s | 526.44 | 524.64 |
| Epoch 3 images/s | 549.79 | 541.90 |
| Epochs 2–3 median images/s | 538.11 | 533.27 |
| Peak Torch allocated memory (MiB) | 2,362.61 | 2,362.30 |
| Best-checkpoint mAP50–95 (%) | 34.504 | 34.127 |

ShardPack is **0.9% slower** in this short pair: effectively similar throughput,
not a demonstrated speed advantage for either loader. Both can supply this AMP
training workload. These sequential, single-seed, two-steady-epoch measurements
are not a statistical equivalence test. The short-run mAP gap is −0.377 points;
do not substitute this for the earlier ten-epoch alignment experiment.

[Map AMP raw result](coco-yolo-amp-map.json) records the matching configuration
and checkpoint hash. This run emitted an NMS time-limit warning during the
separate YOLO26n AMP self-check; the AMP check subsequently passed. It was before
timed training, not during the COCO validation passes. Its self-check weight is
archived as `amp-check-map-yolo26n.pt` in the experiment directory.

Reproduce the added arm by using `map` instead of `stream` in the AMP command
below and choosing a fresh run name (the measured run used `amp-check-map-fp16`).

## Reproduce

Use the existing `torch` and `yolo` uv dependency groups and choose new run names:

```bash
cd python
uv run --no-sync python benchmarks/coco_yolo_alignment.py stream \
  --work /data/datasets/shardpack-yolo-alignment-20260929 \
  --model /data/datasets/shardpack-yolo-alignment-20260929/yolo11n.pt \
  --epochs 3 --run-name fp32-repeat
uv run --no-sync python benchmarks/coco_yolo_alignment.py stream \
  --work /data/datasets/shardpack-yolo-alignment-20260929 \
  --model /data/datasets/shardpack-yolo-alignment-20260929/yolo11n.pt \
  --epochs 3 --amp --run-name fp16-repeat
```

The script refuses to overwrite existing runs. [Raw results](coco-yolo-amp.json)
include per-epoch loader diagnostics and actual AMP states. Large checkpoints
and logs remain in the experiment directory under `amp-check-fp32` and
`amp-check-fp16` prefixes.
