"""End-to-end COCO-to-ShardPack writer benchmark."""

from __future__ import annotations

import argparse
import json
import resource
import struct
import tempfile
import time
from array import array
from collections import defaultdict, deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from itertools import islice
from pathlib import Path

import numpy as np

from shardpack import Dataset, DatasetWriter


def gib_per_second(byte_count: int, seconds: float) -> float:
    return byte_count / (1024**3) / seconds


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--coco", type=Path, default=Path("/data/datasets/coco2017"))
    parser.add_argument("--split", choices=("train", "val"), default="val")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--read-workers", type=int, default=8)
    parser.add_argument("--read-prefetch", type=int, default=32)
    parser.add_argument("--read-task-images", type=int, default=32)
    parser.add_argument("--codec", choices=("none", "zstd"), default="none")
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()

    year_split = f"{args.split}2017"
    annotation_path = args.coco / "annotations" / f"instances_{year_split}.json"
    image_root = args.coco / year_split
    args.output.mkdir(parents=True, exist_ok=True)
    staging = args.output / ".staging"
    staging.mkdir(exist_ok=True)

    prepare_started = time.perf_counter()
    document = json.loads(annotation_path.read_bytes())
    annotations: dict[int, list[dict[str, object]]] = defaultdict(list)
    for annotation in document["annotations"]:
        annotations[annotation["image_id"]].append(annotation)
    images = document["images"]
    encoded_annotations = {
        image["id"]: json.dumps(
            {
                "image": image,
                "annotations": annotations[image["id"]],
            },
            separators=(",", ":"),
        ).encode()
        for image in images
    }
    prepare_seconds = time.perf_counter() - prepare_started

    def read_image(image: dict[str, object]) -> bytes:
        return (image_root / str(image["file_name"])).read_bytes()

    def read_chunk(chunk: list[dict[str, object]]) -> list[tuple[dict[str, object], bytes]]:
        return [(image, read_image(image)) for image in chunk]

    def image_stream() -> Iterator[tuple[dict[str, object], bytes]]:
        if args.read_workers == 1:
            for image in images:
                yield image, read_image(image)
            return
        if args.read_workers < 1 or args.read_prefetch < args.read_workers:
            raise ValueError("read workers must be positive and prefetch must cover all workers")
        if args.read_task_images < 1:
            raise ValueError("read-task-images must be positive")
        source = iter(images)
        pending: deque[Future[list[tuple[dict[str, object], bytes]]]] = deque()
        with ThreadPoolExecutor(max_workers=args.read_workers) as executor:
            while chunk := list(islice(source, args.read_task_images)):
                pending.append(executor.submit(read_chunk, chunk))
                if len(pending) == args.read_prefetch:
                    yield from pending.popleft().result()
            while pending:
                yield from pending.popleft().result()

    with tempfile.TemporaryDirectory(prefix="shardpack-coco-") as temporary:
        temporary = Path(temporary)
        config = temporary / "shardpack.toml"
        config.write_text(
            "\n".join(
                (
                    "version = 1",
                    'default_store = "local"',
                    "",
                    "[stores.local]",
                    'type = "filesystem"',
                    f'root = "{args.output}"',
                    "",
                )
            )
        )
        writer = DatasetWriter(
            bytes.fromhex("c0") * 32,
            staging,
            config_path=config,
            target_shard_bytes=1024**3,
            target_block_bytes=16 * 1024**2,
            block_codec=args.codec,
            queue_batches=2,
            max_batch_records=100_000,
            max_batch_bytes=args.batch_bytes + 16 * 1024**2,
        )

        arena = bytearray()
        offsets = array("Q", [0])
        sample_ids = bytearray()
        record_count = 0
        payload_bytes = 0
        jpeg_bytes = 0
        write_seconds = 0.0

        def submit() -> None:
            nonlocal arena, offsets, sample_ids, write_seconds
            if len(offsets) == 1:
                return
            started = time.perf_counter()
            writer.write_batch(
                memoryview(arena),
                np.frombuffer(offsets, dtype=np.uint64),
                memoryview(sample_ids),
            )
            write_seconds += time.perf_counter() - started
            arena = bytearray()
            offsets = array("Q", [0])
            sample_ids = bytearray()

        ingest_started = time.perf_counter()
        for image, jpeg in image_stream():
            metadata = encoded_annotations[image["id"]]
            payload_size = 4 + len(metadata) + len(jpeg)
            if arena and len(arena) + payload_size > args.batch_bytes:
                submit()
            arena.extend(struct.pack("<I", len(metadata)))
            arena.extend(metadata)
            arena.extend(jpeg)
            offsets.append(len(arena))
            sample_ids.extend(int(image["id"]).to_bytes(16, "little"))
            record_count += 1
            payload_bytes += payload_size
            jpeg_bytes += len(jpeg)
        submit()
        finish_started = time.perf_counter()
        layout_key = writer.finish()
        finish_seconds = time.perf_counter() - finish_started
        total_seconds = time.perf_counter() - ingest_started
        produce_seconds = finish_started - ingest_started

        verify_seconds = 0.0
        verified_records = 0
        if args.verify:
            verify_started = time.perf_counter()
            dataset = Dataset(layout_key, config_path=config)
            verified_records = sum(
                1
                for _ in dataset.records(
                    shuffle_max_records=0,
                    shuffle_max_bytes=0,
                    network_concurrency=2,
                )
            )
            verify_seconds = time.perf_counter() - verify_started

    result = {
        "split": year_split,
        "records": record_count,
        "jpeg_bytes": jpeg_bytes,
        "payload_bytes": payload_bytes,
        "prepare_seconds": prepare_seconds,
        "produce_seconds": produce_seconds,
        "produce_gib_s": gib_per_second(payload_bytes, produce_seconds),
        "write_batch_blocked_seconds": write_seconds,
        "finish_seconds": finish_seconds,
        "total_write_seconds": total_seconds,
        "total_write_gib_s": gib_per_second(payload_bytes, total_seconds),
        "verify_records": verified_records,
        "verify_seconds": verify_seconds,
        "verify_gib_s": gib_per_second(payload_bytes, verify_seconds) if verify_seconds else None,
        "max_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
        "layout_key": layout_key,
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
