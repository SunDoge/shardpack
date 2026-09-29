# Ultralytics streaming integration (experimental)

Ultralytics' default `YOLODataset` is map-style: its sampler and augmentation
pipeline can fetch images by index. ShardPack does **not** emulate this API.
Instead, the example replaces `DetectionTrainer.get_dataset()` and
`get_dataloader()`, retaining the model, loss, optimizer and detection validator.
The dataset facade only supplies a length; it has no `__getitem__`.

Reference implementation: [yolo_streaming.py](../python/examples/yolo_streaming.py).
Tested against **ultralytics 8.4.159**, Torch 2.14.0 on CPU. This is an optional
example, not a stable integration API or a dependency of the ShardPack package.
Upstream trainer internals change; rerun the integration test when upgrading.
Ultralytics has its own licensing terms, separate from ShardPack.

## Use

Install the optional test dependency in the existing Python environment:

```bash
cd python
uv pip install --python .venv/bin/python 'ultralytics==8.4.159'
uv run --no-sync pytest -q tests/test_ultralytics_integration.py
```

With `python/examples` on your import path:

```python
from shardpack import ShardPackSource
from yolo_streaming import ShardPackDetectionTrainer

trainer = ShardPackDetectionTrainer(
    train_source=ShardPackSource(
        train_layout, config=config, rank=0, world_size=1,
        shuffle_max_records=1024, shuffle_max_bytes=256 * 1024**2,
    ),
    val_source=ShardPackSource(
        val_layout, config=config, rank=0, world_size=1,
        auto_advance_epoch=False,
    ),
    train_decode=decode_and_augment,
    val_decode=decode_and_letterbox,
    names=["cat", "dog"],
    decode_workers=4,
    overrides=dict(model="yolo11n.yaml", device="0", batch=32, imgsz=640, epochs=100),
)
trainer.train()
print(trainer.train_loader.stats())
```

The callbacks above are application-provided, thread-safe functions. They define
the payload schema and return one dictionary per record:

| Field | Contract |
| --- | --- |
| `img` | CPU RGB `torch.uint8`, CHW, same spatial size within a batch |
| `cls` | Float tensor `(N, 1)`, contiguous class IDs starting at zero |
| `bboxes` | Float tensor `(N, 4)`, normalized `cx, cy, width, height` **after** resizing/augmentation |
| `im_file` | Unique logical image name (e.g. SampleId hex); no file is opened |
| `ori_shape` | Original `(height, width)` |
| `ratio_pad` | `((gain_h, gain_w), (pad_x, pad_y))` for validation box restoration |

Use aspect-preserving letterbox for validation and retain its exact gain/padding.
Empty annotations use shapes `(0, 1)` and `(0, 4)`. The collator concatenates
annotations and creates `batch_idx`; the trainer handles `/255` normalization
and device transfer. Do not normalize twice. CUDA loading enables pinning after
collation, but the current integration test only verifies CPU execution.

## Deliberate limits

- Single process, single CPU/GPU only. DDP needs explicit equal-step scheduling;
  it is rejected rather than pretending shard partitioning alone is sufficient.
- Explicit batch size and full layouts; no auto-batch, fraction, class filtering,
  resume, label plots or automatic class weights. Set `max_det` yourself if
  images contain many objects: there is no global label scan to infer it.
- Augmentation belongs to the callback. YOLO's default Mosaic/random-index
  augmentation is disabled, and unsupported options fail early. This example
  does not yet wire `SamplePoolSource` or epoch-based Mosaic shutdown.
- Streaming validation runs every requested epoch and re-evaluates the saved
  best checkpoint using the same loader. It does not implement stock checkpoint
  stripping/export finalization, standalone `YOLO.val(data=...)`, JSON evaluation
  export, or the full upstream trainer feature matrix.
- Counts come from layout metadata; batches retain the final partial batch.
  Sources must yield each record once and decoders must not silently skip data.

The test creates independent local train/val `.spk` layouts, runs a real YOLO11n
for two epochs, checks weights changed, checks all five training records in each
epoch, evaluates three validation records, and reloads/evaluates `best.pt`.
It includes empty/multiple annotations and a final partial batch. Randomly
initialized weights and synthetic images test interoperability, not model quality
or throughput. No pretrained checkpoint or external dataset is downloaded.

Upstream references: [custom trainer hooks](https://docs.ultralytics.com/usage/engine/),
[detection trainer](https://docs.ultralytics.com/reference/models/yolo/detect/train/).
