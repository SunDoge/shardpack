"""Benchmark real COCO JPEG decode through the threaded loader."""

from __future__ import annotations

import argparse
import json
import resource
import threading
import time
from pathlib import Path

import albumentations as A
import cv2
import numpy as np
import torch

from shardpack import Record, ShardPackSource
from shardpack.pytorch import ThreadedDataLoader


def augmentation(mode: str) -> A.Compose | None:
    if mode == "none":
        return None
    transforms: list[A.BasicTransform] = [
        A.Resize(256, 256),
        A.RandomCrop(224, 224),
        A.HorizontalFlip(p=0.5),
        A.ColorJitter(p=0.8),
    ]
    if mode == "heavy":
        transforms[1:1] = [
            A.Affine(scale=(0.8, 1.2), rotate=(-15, 15), shear=(-8, 8), p=0.7),
            A.GaussianBlur(blur_limit=(3, 7), p=0.3),
        ]
    return A.Compose(transforms)


def decoder(augment: str):
    local = threading.local()

    def decode_coco(record: Record) -> torch.Tensor:
        payload = record.payload
        metadata_size = int.from_bytes(payload[:4], "little")
        jpeg = np.frombuffer(payload[4 + metadata_size :], dtype=np.uint8)
        image = cv2.imdecode(jpeg, cv2.IMREAD_COLOR)
        if image is None:
            raise ValueError("invalid COCO JPEG payload")
        if augment == "none":
            image = cv2.resize(image, (224, 224), interpolation=cv2.INTER_LINEAR)
        else:
            transform = getattr(local, "transform", None)
            if transform is None:
                transform = augmentation(augment)
                local.transform = transform
            image = transform(image=image)["image"]
        return torch.from_numpy(np.ascontiguousarray(image.transpose(2, 0, 1)))

    return decode_coco


def run_threaded_epoch(
    config: Path,
    layout: str,
    cache: Path,
    workers: int,
    shuffle_records: int,
    shuffle_bytes: int,
    pin_memory: bool,
    device: str,
    augment: str,
) -> dict[str, float | int | str]:
    source = ShardPackSource(
        layout,
        config_path=config,
        cache_dir=cache,
        network_concurrency=2,
        network_prefetch_bytes=128 * 1024**2,
        shuffle_max_records=shuffle_records,
        shuffle_max_bytes=shuffle_bytes,
        shuffle_seed=0xC0C0,
    )
    transfer_stream = torch.cuda.Stream() if device == "cuda" else None

    def transfer(batch: torch.Tensor) -> torch.Tensor:
        if transfer_stream is None:
            return batch
        with torch.cuda.stream(transfer_stream):
            return batch.to("cuda", non_blocking=True)

    loader = ThreadedDataLoader(
        source,
        decode=decoder(augment),
        batch_size=64,
        decode_workers=workers,
        decode_prefetch=128,
        decode_task_records=8,
        prefetch_batches=2,
        pin_memory=pin_memory,
        consumer_fn=transfer,
    )
    started = time.perf_counter()
    records = sum(batch.shape[0] for batch in loader)
    if transfer_stream is not None:
        transfer_stream.synchronize()
    seconds = time.perf_counter() - started
    return {
        "loader": "threaded",
        "workers": workers,
        "shuffle_records": shuffle_records,
        "shuffle_mib": shuffle_bytes // 1024**2,
        "pin_memory": pin_memory,
        "device": device,
        "augment": augment,
        "records": records,
        "seconds": seconds,
        "records_per_second": records / seconds,
        "max_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--layout", required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--shuffle-records", type=int, default=0)
    parser.add_argument("--shuffle-bytes", type=int, default=0)
    parser.add_argument("--pin-memory", action="store_true")
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cpu")
    parser.add_argument("--augment", choices=("none", "light", "heavy"), default="none")
    args = parser.parse_args()

    # Native libraries otherwise create large nested pools underneath every
    # Python decode worker.
    cv2.setNumThreads(1)
    torch.set_num_threads(1)

    for workers in args.workers:
        print(
            json.dumps(
                run_threaded_epoch(
                    args.config,
                    args.layout,
                    args.cache,
                    workers,
                    args.shuffle_records,
                    args.shuffle_bytes,
                    args.pin_memory,
                    args.device,
                    args.augment,
                )
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
