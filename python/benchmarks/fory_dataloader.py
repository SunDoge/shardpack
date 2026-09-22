from __future__ import annotations

import argparse
import tempfile
import time
from array import array
from pathlib import Path

import pyfory
import torch

from shardpack import Dataset, DatasetWriter, Record, ShardPackSource, ThreadedLoader

FORY = pyfory.Fory()


def sample(index: int) -> dict[str, object]:
    return {
        "index": index,
        "label": index % 1000,
        "width": 1920,
        "height": 1080,
        "caption": f"synthetic training sample number {index:08d}",
        "boxes": [[10, 20, 300, 400], [500, 120, 900, 700]],
    }


def decode(record: Record) -> dict[str, object]:
    return FORY.deserialize(pyfory.Buffer(record.payload))


def rate(count: int, seconds: float) -> str:
    return f"{count / seconds:,.0f} records/s"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=100_000)
    parser.add_argument("--batch-records", type=int, default=4096)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="shardpack-fory-bench-") as temporary:
        root = Path(temporary)
        objects = root / "objects"
        config = root / "shardpack.toml"
        config.write_text(
            f'''\
version = 1
default_store = "local"

[stores.local]
type = "filesystem"
root = "{objects}"
'''
        )

        started = time.perf_counter()
        encoded = [FORY.serialize(sample(index)) for index in range(args.records)]
        serialize_seconds = time.perf_counter() - started
        encoded_bytes = sum(map(len, encoded))
        print(
            "Fory encode:      ",
            rate(args.records, serialize_seconds),
            f"{encoded_bytes / serialize_seconds / 1024**2:,.1f} MiB/s",
            f"({encoded_bytes / args.records:,.1f} bytes/record)",
        )

        writer = DatasetWriter(
            config,
            bytes([0xB1]) * 32,
            root / "staging",
            target_shard_bytes=64 * 1024**2,
            target_block_bytes=16 * 1024**2,
            block_codec="zstd",
            zstd_level=1,
            queue_batches=2,
        )
        started = time.perf_counter()
        for begin in range(0, args.records, args.batch_records):
            payloads = encoded[begin : begin + args.batch_records]
            offsets = array("Q", [0])
            for payload in payloads:
                offsets.append(offsets[-1] + len(payload))
            sample_ids = b"".join(
                index.to_bytes(16, "little") for index in range(begin, begin + len(payloads))
            )
            writer.write_batch(memoryview(b"".join(payloads)), offsets, memoryview(sample_ids))
        layout = writer.finish()
        write_seconds = time.perf_counter() - started
        dataset = Dataset(config, layout)
        print(
            "ShardPack write:  ",
            rate(args.records, write_seconds),
            f"{encoded_bytes / write_seconds / 1024**2:,.1f} MiB/s input,",
            f"{dataset.stored_bytes / 1024**2:,.1f} MiB stored",
        )
        del encoded

        reader = dataset.records(
            cache_dir=root / "cache",
            network_concurrency=2,
            network_prefetch_bytes=128 * 1024**2,
        )
        started = time.perf_counter()
        count = sum(1 for record in reader if decode(record)["index"] >= 0)
        direct_seconds = time.perf_counter() - started
        print("Direct decode:    ", rate(count, direct_seconds))

        source = ShardPackSource(
            config,
            layout,
            cache_dir=root / "cache",
            network_concurrency=2,
            network_prefetch_bytes=128 * 1024**2,
        )
        loader = ThreadedLoader(
            source,
            decode=decode,
            batch_size=256,
            decode_workers=max(1, args.workers),
            collate_fn=torch.utils.data.default_collate,
        )
        started = time.perf_counter()
        count = sum(len(batch["index"]) for batch in loader)
        dataloader_seconds = time.perf_counter() - started
        print(
            f"ThreadedLoader ({args.workers}w):",
            rate(count, dataloader_seconds),
            f"including collation ({dataloader_seconds:.3f}s)",
        )


if __name__ == "__main__":
    main()
