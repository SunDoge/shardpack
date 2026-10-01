"""Measure complete remote epochs with fresh caches; never delete test data."""

import argparse
import json
import time
from pathlib import Path

from coco_s3_prepare import scoped_config

from shardpack import Dataset, ShardPackSource


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-spec", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--concurrency", type=int, required=True)
    parser.add_argument("--prefetch-mib", type=int, required=True)
    args = parser.parse_args()
    if args.concurrency < 1 or args.prefetch_mib < 1:
        parser.error("concurrency and prefetch-mib must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    spec = json.loads(args.source_spec.read_text())
    if spec["state"] != "ready":
        raise ValueError("source must be ready")
    config = scoped_config(spec)
    dataset = Dataset(spec["train_layout"], config=config)
    if dataset.stored_bytes > 32 * 1024**3:
        raise ValueError("dataset exceeds the benchmark's 32 GiB cache budget")
    source = ShardPackSource(
        spec["train_layout"],
        config=config,
        rank=0,
        world_size=1,
        shard_seed=42,
        shuffle_seed=42,
        auto_advance_epoch=False,
        shuffle_max_records=1024,
        shuffle_max_bytes=256 * 1024**2,
        network_concurrency=args.concurrency,
        network_prefetch_bytes=args.prefetch_mib * 1024**2,
        cache_dir=args.output / "cache",
        cache_mode="plaintext",
        cache_max_bytes=32 * 1024**3,
        encryption_key_id=spec["encryption_key_id"],
        encryption_key_path=spec["encryption_key_path"],
    )
    rows = []
    for mode in ("cold", "warm"):
        started = time.perf_counter()
        seen = set()
        first = None
        for record in source:
            if first is None:
                first = time.perf_counter() - started
            assert record.sample_id not in seen
            seen.add(record.sample_id)
        elapsed = time.perf_counter() - started
        assert len(seen) == dataset.record_count
        rows.append(
            {
                "mode": mode,
                "seconds": elapsed,
                "first_record_seconds": first,
                "records": len(seen),
                "records_per_second": len(seen) / elapsed,
                "metrics": source.stats(),
            }
        )
        print(json.dumps(rows[-1]), flush=True)
        result = {
            "concurrency": args.concurrency,
            "prefetch_mib": args.prefetch_mib,
            "stored_bytes": dataset.stored_bytes,
            "epochs": rows,
        }
        (args.output / "result.json").write_text(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
