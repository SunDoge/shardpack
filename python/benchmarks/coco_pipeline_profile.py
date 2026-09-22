"""Isolate bottlenecks in the COCO threaded training pipeline."""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import cv2
import numpy as np
import torch

from shardpack import Dataset, Record, ShardPackSource
from shardpack.pytorch import ThreadedDataLoader


def jpeg_bytes(record: Record) -> np.ndarray:
    metadata_size = int.from_bytes(record.payload[:4], "little")
    return np.frombuffer(record.payload[4 + metadata_size :], dtype=np.uint8)


def metadata_only(record: Record) -> int:
    return len(record.payload)


def jpeg_only(record: Record) -> int:
    image = cv2.imdecode(jpeg_bytes(record), cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError("invalid COCO JPEG payload")
    return image.shape[0] * image.shape[1]


def decode_resize(record: Record) -> torch.Tensor:
    image = cv2.imdecode(jpeg_bytes(record), cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError("invalid COCO JPEG payload")
    image = cv2.resize(image, (224, 224), interpolation=cv2.INTER_LINEAR)
    return torch.from_numpy(image).permute(2, 0, 1)


def keep_list(samples: list[Any]) -> list[Any]:
    return samples


def source(args: argparse.Namespace) -> ShardPackSource:
    return ShardPackSource(
        args.config,
        args.layout,
        cache_dir=args.cache,
        network_concurrency=2,
        network_prefetch_bytes=128 * 1024**2,
    )


def measure(name: str, operation: Callable[[], int]) -> None:
    started = time.perf_counter()
    records = operation()
    seconds = time.perf_counter() - started
    print(
        json.dumps(
            {
                "stage": name,
                "records": records,
                "seconds": seconds,
                "records_per_second": records / seconds,
            }
        ),
        flush=True,
    )


def consume_threaded(
    args: argparse.Namespace,
    decode: Callable[[Record], Any],
    collate: Callable[[list[Any]], Any] | None,
) -> int:
    loader = ThreadedDataLoader(
        source(args),
        decode=decode,
        batch_size=64,
        decode_workers=args.workers,
        decode_prefetch=128,
        decode_task_records=8,
        prefetch_batches=2,
        collate_fn=collate,
    )
    return sum(len(batch) for batch in loader)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--layout", required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    cv2.setNumThreads(1)
    torch.set_num_threads(1)

    dataset = Dataset(args.config, args.layout)
    measure("rust-record-stream", lambda: sum(1 for _ in dataset.records(cache_dir=args.cache)))
    measure("thread-scheduling", lambda: consume_threaded(args, metadata_only, None))
    measure("jpeg-decode", lambda: consume_threaded(args, jpeg_only, None))
    measure("jpeg-resize-no-collate", lambda: consume_threaded(args, decode_resize, keep_list))
    measure("jpeg-resize-tensor-collate", lambda: consume_threaded(args, decode_resize, None))


if __name__ == "__main__":
    main()
