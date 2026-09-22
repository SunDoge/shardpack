"""Benchmark a CPU-only NVIDIA DALI pipeline on COCO JPEG files."""

from __future__ import annotations

import argparse
import json
import resource
import tempfile
import time
from pathlib import Path

from nvidia.dali import fn, pipeline_def, types
from nvidia.dali.plugin.pytorch import DALIGenericIterator, LastBatchPolicy


@pipeline_def
def coco_pipeline(file_list: str, augment: str):
    encoded, _ = fn.readers.file(
        file_list=file_list,
        random_shuffle=False,
        name="Reader",
    )
    images = fn.decoders.image(
        encoded,
        device="cpu",
        output_type=types.BGR,
        use_fast_idct=True,
    )
    if augment == "none":
        images = fn.resize(images, resize_x=224, resize_y=224)
    else:
        images = fn.resize(images, resize_x=256, resize_y=256)
        positions = fn.random.uniform(range=(0.0, 1.0), shape=(2,))
        images = fn.crop(
            images,
            crop=(224, 224),
            crop_pos_x=positions[0],
            crop_pos_y=positions[1],
        )
        images = fn.flip(images, horizontal=fn.random.coin_flip(probability=0.5))
        images = fn.color_twist(
            images,
            brightness=fn.random.uniform(range=(0.8, 1.2)),
            contrast=fn.random.uniform(range=(0.8, 1.2)),
            saturation=fn.random.uniform(range=(0.8, 1.2)),
            hue=fn.random.uniform(range=(-0.2, 0.2)),
        )
    return fn.transpose(images, perm=(2, 0, 1))


def run(root: Path, workers: int, augment: str, epochs: int) -> dict[str, object]:
    images = sorted(root.glob("*.jpg"))
    if not images:
        raise ValueError(f"no JPEG images in {root}")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt") as listing:
        for image in images:
            listing.write(f"{image} 0\n")
        listing.flush()

        pipeline = coco_pipeline(
            batch_size=64,
            num_threads=workers,
            device_id=0,
            seed=0xC0C0,
            prefetch_queue_depth=2,
            file_list=listing.name,
            augment=augment,
        )
        iterator = DALIGenericIterator(
            pipeline,
            output_map=["images"],
            reader_name="Reader",
            auto_reset=True,
            last_batch_policy=LastBatchPolicy.PARTIAL,
        )

        measurements = []
        for epoch in range(epochs + 1):
            started = time.perf_counter()
            records = sum(batch[0]["images"].shape[0] for batch in iterator)
            seconds = time.perf_counter() - started
            if epoch:
                measurements.append((records, seconds))

    total_records = sum(records for records, _ in measurements)
    total_seconds = sum(seconds for _, seconds in measurements)
    return {
        "loader": "dali-cpu",
        "dali_version": __import__("nvidia.dali").dali.__version__,
        "workers": workers,
        "augment": augment,
        "epochs": epochs,
        "records_per_epoch": len(images),
        "seconds_per_epoch": total_seconds / epochs,
        "records_per_second": total_records / total_seconds,
        "max_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--images", type=Path, required=True)
    parser.add_argument("--workers", type=int, nargs="+", default=[4, 8])
    parser.add_argument("--augment", choices=("none", "light"), default="none")
    parser.add_argument("--epochs", type=int, default=3)
    args = parser.parse_args()

    for workers in args.workers:
        print(json.dumps(run(args.images, workers, args.augment, args.epochs)), flush=True)


if __name__ == "__main__":
    main()
