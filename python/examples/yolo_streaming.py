"""Experimental, single-process Ultralytics 8.4.159 detection adapter.

Payload decoding/augmentation belongs to the caller, not the container format.
See docs/ultralytics.md. This module is intentionally outside the core package.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass

import torch
from ultralytics.models.yolo.detect import DetectionTrainer
from ultralytics.nn.tasks import load_checkpoint

from shardpack import Dataset, ShardPackSource
from shardpack.pytorch import ThreadedDataLoader


@dataclass
class StreamMetadata:
    """Length only: deliberately no __getitem__ or image-file index."""

    count: int

    def __len__(self):
        return self.count


def detection_collate(samples):
    """Stack RGB CHW uint8 images; boxes are normalized xywh after augmentation."""
    return {
        "img": torch.stack([s["img"] for s in samples]),
        "cls": torch.cat([s["cls"].reshape(-1, 1) for s in samples]),
        "bboxes": torch.cat([s["bboxes"].reshape(-1, 4) for s in samples]),
        "batch_idx": torch.cat(
            [torch.full((s["cls"].numel(),), i, dtype=torch.int64) for i, s in enumerate(samples)]
        ),
        **{key: [s[key] for s in samples] for key in ("im_file", "ori_shape", "ratio_pad")},
    }


class StreamingDetectionLoader(ThreadedDataLoader):
    def __init__(self, source, *, count, **kwargs):
        super().__init__(source, collate_fn=detection_collate, **kwargs)
        self.dataset = StreamMetadata(count)
        # Upstream uses len(sampler) for final-batch checks, not to read samples.
        self.sampler = range(count)
        self.num_workers = self.decode_workers

    def __len__(self):
        return math.ceil(len(self.dataset) / self.batch_size)

    def reset(self):
        # ThreadedDataLoader starts a fresh iterator on every __iter__.
        pass


class ShardPackDetectionTrainer(DetectionTrainer):
    """Use explicit sources and decode callbacks instead of image-directory YAML.

    Each decode callback returns img, cls, bboxes, im_file, ori_shape, ratio_pad.
    Only single CPU/GPU training is supported by this proof-of-integration.
    """

    def __init__(
        self,
        *,
        train_source,
        val_source,
        train_decode,
        val_decode,
        names,
        decode_workers=4,
        overrides=None,
    ):
        options = dict(overrides or {})
        # Reject features whose default implementation needs map-style metadata,
        # or whose configuration would silently be ignored by our decode callback.
        unsupported = {
            "plots": False,
            "rect": False,
            "cache": False,
            "resume": False,
            "save_json": False,
            "single_cls": False,
            "classes": None,
            "fraction": 1.0,
            "cls_pw": 0.0,
            "compile": False,
            "close_mosaic": 0,
            "mosaic": 0.0,
            "mixup": 0.0,
            "copy_paste": 0.0,
            "cutmix": 0.0,
            "bgr": 0.0,
            "hsv_h": 0.0,
            "hsv_s": 0.0,
            "hsv_v": 0.0,
            "degrees": 0.0,
            "translate": 0.0,
            "scale": 0.0,
            "shear": 0.0,
            "perspective": 0.0,
            "flipud": 0.0,
            "fliplr": 0.0,
            "augmentations": None,
            "task": "detect",
            "split": "val",
        }
        for key, value in unsupported.items():
            if key in options and options[key] != value:
                raise ValueError(f"streaming example requires {key}={value!r}")
            options[key] = value
        if options.get("batch", 16) < 1:
            raise ValueError("automatic batch sizing requires map-style labels; set batch >= 1")
        device = options.get("device", "cpu")
        if (
            int(os.environ.get("WORLD_SIZE", "1")) != 1
            or isinstance(device, (list, tuple))
            or "," in str(device)
        ):
            raise ValueError("this adapter currently supports one process / one device only")
        for source in (train_source, val_source):
            if not isinstance(source, ShardPackSource) or source._distributed_context() != (0, 1):
                raise ValueError(
                    "provide unpartitioned ShardPackSource objects (rank=0, world_size=1)"
                )
        if not names:
            raise ValueError("names must contain at least one class")
        if decode_workers < 1:
            raise ValueError("decode_workers must be positive")
        self.stream_sources = {"train": train_source, "val": val_source}
        self.stream_decoders = {"train": train_decode, "val": val_decode}
        self.stream_names = dict(enumerate(names))
        self.stream_workers = decode_workers
        options.setdefault("device", "cpu")
        options["data"] = "shardpack-stream"
        super().__init__(overrides=options)

    def get_dataset(self):
        return {
            "train": "train",
            "val": "val",
            "nc": len(self.stream_names),
            "names": self.stream_names,
            "channels": 3,
        }

    def get_dataloader(self, dataset_path, batch_size=16, rank=0, mode="train"):
        source = self.stream_sources[mode]
        count = Dataset(
            source.layout, config=source.config, config_path=source.config_path, store=source.store
        ).record_count
        if count == 0:
            raise ValueError(f"{mode} layout is empty")
        return StreamingDetectionLoader(
            source,
            count=count,
            decode=self.stream_decoders[mode],
            batch_size=batch_size,
            decode_workers=self.stream_workers,
            decode_task_records=max(1, min(8, batch_size // self.stream_workers)),
            decode_prefetch=max(2 * batch_size, 2 * self.stream_workers),
            pin_memory=self.device.type == "cuda",
            enable_stats=True,
        )

    def final_eval(self):
        # Base final_eval opens a standalone validator and rechecks a disk YAML.
        # Evaluate the saved best checkpoint through the existing streaming path.
        if self.best.exists():
            previous = self.ema.ema
            self.ema.ema, _ = load_checkpoint(self.best, device=self.device)
            self.ema.ema.args = self.args
            try:
                self.metrics, _ = self.validate()
            finally:
                self.ema.ema = previous
