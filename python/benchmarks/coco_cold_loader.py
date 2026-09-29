"""Linux cold/warm loader comparison using targeted POSIX_FADV_DONTNEED.

This consumes pinned CPU batches, with no model or GPU transfers. Each invocation
creates one loader and reads a cold pass then a warm pass with persistent workers.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import cv2
import psutil
import torch
from coco_yolo_alignment import Decode, MapDataset, config, worker_init
from yolo_streaming import StreamingDetectionLoader, detection_collate

from shardpack import ShardPackSource


def process_counters():
    parent = psutil.Process()
    counters = {}
    for process in [parent, *parent.children(recursive=True)]:
        try:
            cpu = process.cpu_times()
            counters[process.pid] = {
                "read_bytes": process.io_counters().read_bytes,
                "cpu_seconds": cpu.user + cpu.system,
            }
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return counters


def evict(paths):
    count = size = 0
    started = time.perf_counter()
    for path in paths:
        with path.open("rb", buffering=0) as file:
            size += os.fstat(file.fileno()).st_size
            os.posix_fadvise(file.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        count += 1
    return {"files": count, "bytes": size, "seconds": time.perf_counter() - started}


def measure(loader, expected_ids, mode, temperature):
    before = process_counters()
    started = time.perf_counter()
    iterator = iter(loader)  # include worker startup, prefetch and initial pool fill
    first = None
    count = batches = 0
    seen = set()
    for batch in iterator:
        if first is None:
            first = time.perf_counter() - started
        assert batch["img"].is_pinned()
        count += len(batch["img"])
        batches += 1
        for name in batch["im_file"]:
            sample_id = int(Path(name).stem)
            assert sample_id not in seen
            seen.add(sample_id)
    elapsed = time.perf_counter() - started
    after = process_counters()  # before persistent workers are shut down
    assert seen == expected_ids
    delta = {
        key: sum(values[key] - before.get(pid, {}).get(key, 0) for pid, values in after.items())
        for key in ("read_bytes", "cpu_seconds")
    }
    result = {
        "mode": mode,
        "pass": temperature,
        "records": count,
        "batches": batches,
        "seconds": elapsed,
        "images_per_second": count / elapsed,
        "first_batch_seconds": first,
        "process_tree": delta,
    }
    if mode == "stream":
        result["loader"] = loader.stats()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("map", "stream"))
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--round", type=int, required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch", type=int, default=16)
    args = parser.parse_args()
    output = args.work / f"cold-loader-{args.mode}-{args.round}.json"
    if output.exists():
        raise ValueError(f"refusing to overwrite {output}")
    cv2.setNumThreads(1)
    torch.set_num_threads(2)
    # Initialize CUDA/pinned allocator before timing; no model runs on the GPU.
    torch.empty(1, pin_memory=True)
    manifest = json.loads((args.work / "experiment.json").read_text())
    rows = manifest["splits"]["train"]["rows"]
    image_root = Path(manifest["coco"]) / "train2017"
    if args.mode == "map":
        loader = torch.utils.data.DataLoader(
            MapDataset(rows, image_root, Decode(640)),
            batch_size=args.batch,
            shuffle=True,
            num_workers=args.workers,
            persistent_workers=True,
            pin_memory=True,
            collate_fn=detection_collate,
            worker_init_fn=worker_init,
            multiprocessing_context="spawn",
            prefetch_factor=2,
            generator=torch.Generator().manual_seed(42),
        )
        eviction = evict(image_root / row["file_name"] for row in rows)
    else:
        source = ShardPackSource(
            manifest["splits"]["train"]["layout"],
            config=config(args.work),
            rank=0,
            world_size=1,
            shard_seed=42,
            shuffle_seed=42,
            shuffle_max_records=1024,
            shuffle_max_bytes=256 * 1024**2,
        )
        loader = StreamingDetectionLoader(
            source,
            count=len(rows),
            decode=Decode(640),
            batch_size=args.batch,
            decode_workers=args.workers,
            decode_task_records=max(1, min(8, args.batch // args.workers)),
            decode_prefetch=max(2 * args.batch, 2 * args.workers),
            pin_memory=True,
            enable_stats=True,
        )
        # Only objects of this experiment; also evicts its unused validation shards.
        eviction = evict(sorted((args.work / "objects").rglob("*.spk")))
    expected_ids = {row["id"] for row in rows}
    results = [
        measure(loader, expected_ids, args.mode, temperature) for temperature in ("cold", "warm")
    ]
    report = {
        "round": args.round,
        "mode": args.mode,
        "eviction": eviction,
        "results": results,
        "batch": args.batch,
        "workers": args.workers,
        "note": "advisory eviction; proc IO includes worker startup/library reads",
    }
    output.write_text(json.dumps(report, indent=2))
    print(json.dumps({k: v for k, v in report.items() if k != "results"}))
    for result in results:
        print(json.dumps({k: v for k, v in result.items() if k != "loader"}), flush=True)


if __name__ == "__main__":
    main()
