"""Measure COCO source/loader throughput, CPU time, first item and process RSS.

Use a fresh process per mode/version; --native selects a separately built native
extension for paired comparisons without replacing the installed package.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import resource
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--layout", required=True)
    parser.add_argument("--mode", choices=("records", "threaded"), required=True)
    parser.add_argument("--native", type=Path)
    parser.add_argument("--cache", type=Path)
    parser.add_argument("--shuffle-records", type=int, default=1024)
    parser.add_argument("--shuffle-bytes", type=int, default=256 * 1024**2)
    args = parser.parse_args()
    if args.native:
        spec = importlib.util.spec_from_file_location("shardpack._native", args.native)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

    from shardpack import Dataset, ShardPackSource

    config = {
        "default_store": "local",
        "stores": {
            "local": {
                "type": "filesystem",
                "root": args.root,
            }
        },
    }
    options = {
        "cache_dir": args.cache,
        "network_concurrency": 2,
        "network_prefetch_bytes": 64 * 1024**2,
        "shuffle_max_records": args.shuffle_records,
        "shuffle_max_bytes": args.shuffle_bytes if args.shuffle_records else 0,
        "shuffle_seed": 7,
    }
    dataset = Dataset(args.layout, config=config)
    if args.mode == "records":
        loader = dataset.records(**options)
    else:
        import cv2
        import numpy as np
        import torch

        from shardpack.pytorch import ThreadedDataLoader

        cv2.setNumThreads(1)
        torch.set_num_threads(1)

        def decode(record):
            offset = 4 + int.from_bytes(record.payload[:4], "little")
            image = cv2.imdecode(
                np.frombuffer(record.payload[offset:], dtype=np.uint8), cv2.IMREAD_COLOR
            )
            if image is None:
                raise ValueError("invalid JPEG")
            image = cv2.resize(image, (224, 224))
            return torch.from_numpy(np.ascontiguousarray(image.transpose(2, 0, 1)))

        source = ShardPackSource(args.layout, config=config, **options)
        loader = ThreadedDataLoader(
            source,
            decode=decode,
            batch_size=64,
            decode_workers=4,
            decode_prefetch=128,
            decode_task_records=8,
            prefetch_batches=2,
        )
    cpu_start = resource.getrusage(resource.RUSAGE_SELF)
    start = time.perf_counter()
    count = 0
    first_seconds = None
    for item in loader:
        if first_seconds is None:
            first_seconds = time.perf_counter() - start
        count += 1 if args.mode == "records" else len(item)
    seconds = time.perf_counter() - start
    usage = resource.getrusage(resource.RUSAGE_SELF)
    assert count == dataset.record_count
    print(
        json.dumps(
            {
                "mode": args.mode,
                "records": count,
                "seconds": seconds,
                "records_per_second": count / seconds,
                "first_item_seconds": first_seconds,
                "cpu_seconds": (
                    usage.ru_utime + usage.ru_stime - cpu_start.ru_utime - cpu_start.ru_stime
                ),
                "max_rss_mib": usage.ru_maxrss / 1024,
                "shuffle_records": args.shuffle_records,
            }
        )
    )


if __name__ == "__main__":
    main()
