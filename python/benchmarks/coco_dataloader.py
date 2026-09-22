"""Benchmark real COCO JPEG decode through ShardPack and PyTorch DataLoader."""

from __future__ import annotations

import argparse
import json
import resource
import time
from pathlib import Path

import cv2
import numpy as np
import torch

from shardpack import Record
from shardpack.pytorch import ShardPackDataset


def decode_coco(record: Record) -> torch.Tensor:
    payload = record.payload
    metadata_size = int.from_bytes(payload[:4], "little")
    jpeg = np.frombuffer(payload[4 + metadata_size :], dtype=np.uint8)
    image = cv2.imdecode(jpeg, cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError("invalid COCO JPEG payload")
    image = cv2.resize(image, (224, 224), interpolation=cv2.INTER_LINEAR)
    return torch.from_numpy(image).permute(2, 0, 1)


def run_epoch(
    config: Path,
    layout: str,
    cache: Path,
    workers: int,
    shuffle_records: int,
    shuffle_bytes: int,
) -> dict[str, float | int]:
    source = ShardPackDataset(
        config,
        layout,
        decode=decode_coco,
        cache_dir=cache,
        network_concurrency=2,
        network_prefetch_bytes=128 * 1024**2,
        shuffle_max_records=shuffle_records,
        shuffle_max_bytes=shuffle_bytes,
        shuffle_seed=0xC0C0,
    )
    loader = torch.utils.data.DataLoader(
        source,
        batch_size=64,
        num_workers=workers,
        multiprocessing_context="spawn" if workers else None,
        prefetch_factor=2 if workers else None,
        persistent_workers=False,
    )
    started = time.perf_counter()
    records = sum(batch.shape[0] for batch in loader)
    seconds = time.perf_counter() - started
    return {
        "workers": workers,
        "shuffle_records": shuffle_records,
        "shuffle_mib": shuffle_bytes // 1024**2,
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
    parser.add_argument("--workers", type=int, nargs="+", default=[0, 1, 2, 4])
    parser.add_argument("--shuffle-records", type=int, default=0)
    parser.add_argument("--shuffle-bytes", type=int, default=0)
    args = parser.parse_args()

    for workers in args.workers:
        print(
            json.dumps(
                run_epoch(
                    args.config,
                    args.layout,
                    args.cache,
                    workers,
                    args.shuffle_records,
                    args.shuffle_bytes,
                )
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
