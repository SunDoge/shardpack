"""Paired COCO fine-tuning: map-style files vs streaming ShardPack.

Run prepare once, then map and stream in separate processes. See the companion
experiment document for scope: this isolates loading/shuffle, not default Mosaic.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import resource
import statistics
import sys
import time
from array import array
from pathlib import Path

import cv2
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parents[1] / "examples"))
from yolo_streaming import ShardPackDetectionTrainer, detection_collate

from shardpack import Dataset, DatasetWriter, ShardPackSource


def config(root):
    return {
        "default_store": "local",
        "stores": {"local": {"type": "filesystem", "root": root / "objects"}},
    }


def payload(row, image):
    header = json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
    return len(header).to_bytes(4, "little") + header + image


class Decode:
    def __init__(self, size):
        self.size = size

    def __call__(self, record):
        view = record.payload
        n = int.from_bytes(view[:4], "little")
        row = json.loads(bytes(view[4 : 4 + n]))
        return self.image(row, view[4 + n :])

    def image(self, row, encoded):
        image = cv2.imdecode(np.frombuffer(encoded, np.uint8), cv2.IMREAD_COLOR)
        if image is None:
            raise ValueError(f"invalid image {row['id']}")
        h, w = image.shape[:2]
        if (h, w) != (row["height"], row["width"]):
            raise ValueError("annotation/image dimensions differ")
        gain = min(self.size / h, self.size / w)
        new_w, new_h = round(w * gain), round(h * gain)
        image = cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
        dx, dy = (self.size - new_w) / 2, (self.size - new_h) / 2
        left, top = round(dx - 0.1), round(dy - 0.1)
        image = cv2.copyMakeBorder(
            image,
            top,
            round(dy + 0.1),
            left,
            round(dx + 0.1),
            cv2.BORDER_CONSTANT,
            value=(114, 114, 114),
        )
        boxes = np.array(row["boxes"], dtype=np.float32).reshape(-1, 4)
        boxes[:, :2] += boxes[:, 2:] / 2  # absolute xywh -> center xywh
        boxes *= gain
        boxes[:, 0] += left
        boxes[:, 1] += top
        boxes /= self.size
        return {
            "img": torch.from_numpy(np.ascontiguousarray(image[:, :, ::-1].transpose(2, 0, 1))),
            "cls": torch.tensor(row["classes"], dtype=torch.float32).reshape(-1, 1),
            "bboxes": torch.from_numpy(boxes),
            "im_file": f"{row['id']:012d}.jpg",
            "ori_shape": (h, w),
            "ratio_pad": ((gain, gain), (left, top)),
        }


class MapDataset(torch.utils.data.Dataset):
    collate_fn = staticmethod(detection_collate)

    def __init__(self, rows, image_root, decode):
        self.rows, self.image_root, self.decode = rows, image_root, decode

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, index):
        row = self.rows[index]
        return self.decode.image(row, (self.image_root / row["file_name"]).read_bytes())


def worker_init(_):
    cv2.setNumThreads(1)
    torch.set_num_threads(1)


class MapTrainer(ShardPackDetectionTrainer):
    def get_dataloader(self, dataset_path, batch_size=16, rank=0, mode="train"):
        generator = torch.Generator().manual_seed(self.args.seed)
        return torch.utils.data.DataLoader(
            self.map_datasets[mode],
            batch_size=batch_size,
            shuffle=mode == "train",
            num_workers=self.stream_workers,
            collate_fn=detection_collate,
            persistent_workers=True,
            pin_memory=self.device.type == "cuda",
            worker_init_fn=worker_init,
            generator=generator,
            multiprocessing_context="spawn",
            prefetch_factor=2,
        )


def prepare(args):
    args.work.mkdir(parents=True, exist_ok=True)
    manifest_path = args.work / "experiment.json"
    if manifest_path.exists():
        raise ValueError("experiment already exists; use a new work directory")
    result = {"seed": args.seed, "coco": str(args.coco.resolve()), "splits": {}}
    for split, limit in (("train", args.train_count), ("val", args.val_count)):
        annotation = args.coco / "annotations" / f"instances_{split}2017.json"
        raw = annotation.read_bytes()
        data = json.loads(raw)
        cats = sorted(data["categories"], key=lambda c: c["id"])
        mapping = {c["id"]: i for i, c in enumerate(cats)}
        result["names"] = [c["name"] for c in cats]
        images = sorted(data["images"], key=lambda i: i["id"])
        if split == "train":
            random.Random(args.seed).shuffle(images)
        if not 0 < limit <= len(images):
            raise ValueError("requested split count out of range")
        rows = {i["id"]: {**i, "boxes": [], "classes": []} for i in images[:limit]}
        for ann in data["annotations"]:
            if ann["image_id"] not in rows or ann.get("iscrowd", 0):
                continue
            row = rows[ann["image_id"]]
            x, y, w, h = ann["bbox"]
            x1, y1 = max(0, x), max(0, y)
            x2, y2 = min(row["width"], x + w), min(row["height"], y + h)
            if x2 <= x1 or y2 <= y1:
                continue
            box = [x1, y1, x2 - x1, y2 - y1]
            cls = mapping[ann["category_id"]]
            if (cls, box) not in list(zip(row["classes"], row["boxes"])):
                row["classes"].append(cls)
                row["boxes"].append(box)
        selected = list(rows.values())
        definition = json.dumps(selected, sort_keys=True).encode()
        writer = DatasetWriter(
            hashlib.sha256(definition).digest(),
            args.work / f"staging-{split}",
            config=config(args.work),
            target_shard_bytes=128 * 1024**2,
            target_block_bytes=4 * 1024**2,
        )
        started = time.perf_counter()
        for row in selected:
            encoded = payload(row, (args.coco / f"{split}2017" / row["file_name"]).read_bytes())
            writer.write_batch(
                encoded, array("Q", [0, len(encoded)]), row["id"].to_bytes(16, "little")
            )
        layout = writer.finish()
        result["splits"][split] = {
            "rows": selected,
            "layout": layout,
            "annotation_sha256": hashlib.sha256(raw).hexdigest(),
            "selection_sha256": hashlib.sha256(definition).hexdigest(),
            "pack_seconds": time.perf_counter() - started,
        }
        print(f"packed {split}: {len(selected)} records", flush=True)
    manifest_path.write_text(json.dumps(result))
    verify(args, result)


def verify(args, manifest):
    decode = Decode(args.imgsz)
    for split, info in manifest["splits"].items():
        expected = {r["id"]: r for r in info["rows"]}
        seen = set()
        for record in Dataset(info["layout"], config=config(args.work)).records():
            sid = int.from_bytes(record.sample_id, "little")
            assert sid not in seen
            row = expected[sid]
            raw = (Path(manifest["coco"]) / f"{split}2017" / row["file_name"]).read_bytes()
            assert bytes(record.payload) == payload(row, raw)
            if len(seen) < 128:
                a, b = decode(record), decode.image(row, raw)
                for key in ("img", "cls", "bboxes"):
                    assert torch.equal(a[key], b[key]), (sid, key)
            seen.add(sid)
        assert seen == expected.keys()
        print(
            f"verified {split}: all payloads/IDs; first 128 decoded tensors identical", flush=True
        )


def run(args):
    import ultralytics

    run_name = args.run_name or args.mode
    if Path(run_name).name != run_name or run_name in {".", ".."}:
        raise ValueError("run-name must be a single filename component")
    if (
        any((args.work / f"{run_name}-{suffix}.json").exists() for suffix in ("epochs", "result"))
        or (args.work / "runs" / run_name).exists()
    ):
        raise ValueError("run already exists; choose a new --run-name")
    manifest = json.loads((args.work / "experiment.json").read_text())
    decode = Decode(args.imgsz)
    sources = {
        split: ShardPackSource(
            info["layout"],
            config=config(args.work),
            rank=0,
            world_size=1,
            shard_seed=args.seed,
            shuffle_seed=args.seed,
            shuffle_max_records=1024 if split == "train" else 0,
            shuffle_max_bytes=256 * 1024**2 if split == "train" else 0,
            auto_advance_epoch=split == "train",
        )
        for split, info in manifest["splits"].items()
    }
    if args.source_spec:
        from coco_s3_prepare import scoped_config

        if args.mode != "stream":
            raise ValueError("remote source spec requires stream mode")
        spec = json.loads(Path(args.source_spec).read_text())
        if spec["state"] != "ready":
            raise ValueError("remote source is not ready")
        sources["train"] = ShardPackSource(
            spec["train_layout"],
            config=scoped_config(spec),
            rank=0,
            world_size=1,
            shard_seed=args.seed,
            shuffle_seed=args.seed,
            shuffle_max_records=1024,
            shuffle_max_bytes=256 * 1024**2,
            cache_dir=spec["cache_dir"],
            cache_mode="plaintext",
            cache_max_bytes=8 * 1024**3,
            encryption_key_id=spec["encryption_key_id"],
            encryption_key_path=spec["encryption_key_path"],
        )
    trainer_type = MapTrainer if args.mode == "map" else ShardPackDetectionTrainer
    trainer = trainer_type(
        train_source=sources["train"],
        val_source=sources["val"],
        train_decode=decode,
        val_decode=decode,
        names=manifest["names"],
        decode_workers=args.workers,
        overrides={
            "model": args.model,
            "device": args.device,
            "epochs": args.epochs,
            "batch": args.batch,
            "imgsz": args.imgsz,
            "seed": args.seed,
            "amp": args.amp,
            "deterministic": True,
            "optimizer": "AdamW",
            "lr0": 0.0001,
            "warmup_bias_lr": 0.0,
            "nbs": args.batch,
            "warmup_epochs": 1,
            "patience": 0,
            "workers": args.workers,
            "project": str(args.work / "runs"),
            "name": run_name,
            "exist_ok": False,
        },
    )
    if args.mode == "map":
        trainer.map_datasets = {
            split: MapDataset(info["rows"], Path(manifest["coco"]) / f"{split}2017", decode)
            for split, info in manifest["splits"].items()
        }
    state = {}
    epochs = []

    def begin(t):
        if t.batch_size != args.batch:
            raise RuntimeError("trainer changed batch size (e.g. OOM recovery); trial is invalid")
        if bool(t.amp) != args.amp:
            raise RuntimeError("resolved AMP mode differs from requested mode")
        if torch.cuda.is_available():
            torch.cuda.synchronize()
            torch.cuda.reset_peak_memory_stats()
        state.update(start=time.perf_counter(), count=0, pinned=True)

    def batch_start(t):
        if state["count"] == 0:
            state["first_batch_seconds"] = time.perf_counter() - state["start"]
            state["first_batch_source"] = (
                t.train_loader.stats().get("source", {}) if args.mode == "stream" else {}
            )

    def batch(t):
        state["count"] += 1

    def end(t):
        if torch.cuda.is_available():
            torch.cuda.synchronize()
        seconds = time.perf_counter() - state["start"]
        expected_steps = (len(manifest["splits"]["train"]["rows"]) + args.batch - 1) // args.batch
        assert state["count"] == expected_steps
        epochs.append(
            {
                "epoch": t.epoch,
                "train_seconds": seconds,
                "images_per_second": len(manifest["splits"]["train"]["rows"]) / seconds,
                "batches": state["count"],
                "amp_enabled": bool(t.amp),
                "first_batch_seconds": state["first_batch_seconds"],
                "first_batch_source": state["first_batch_source"],
                "gpu_peak_allocated_mib": torch.cuda.max_memory_allocated() / 1024**2
                if torch.cuda.is_available()
                else 0,
            }
        )
        if args.mode == "stream":
            stats = t.train_loader.stats()
            assert stats["source"]["records_read"] == len(manifest["splits"]["train"]["rows"])
            epochs[-1]["loader"] = stats
        print("ALIGNMENT_EPOCH " + json.dumps(epochs[-1]), flush=True)

    def fit_end(t):
        epochs[-1]["metrics"] = {k: float(v) for k, v in t.metrics.items()}
        (args.work / f"{run_name}-epochs.json").write_text(json.dumps(epochs, indent=2))

    trainer.add_callback("on_train_epoch_start", begin)
    trainer.add_callback("on_train_batch_start", batch_start)
    trainer.add_callback("on_train_batch_end", batch)
    trainer.add_callback("on_train_epoch_end", end)
    trainer.add_callback("on_fit_epoch_end", fit_end)
    start = time.perf_counter()
    trainer.train()
    result = {
        "mode": args.mode,
        "ultralytics": ultralytics.__version__,
        "torch": torch.__version__,
        "options": vars(args) | {"coco": str(args.coco), "work": str(args.work)},
        "wall_seconds": time.perf_counter() - start,
        "epochs": epochs,
        "metrics": {k: float(v) for k, v in trainer.metrics.items()},
        "parent_max_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
        "best": str(trainer.best),
    }
    if Path(args.model).exists():
        result["initial_checkpoint_sha256"] = hashlib.sha256(
            Path(args.model).read_bytes()
        ).hexdigest()
    (args.work / f"{run_name}-result.json").write_text(json.dumps(result, indent=2))
    print("ALIGNMENT_RESULT " + json.dumps(result), flush=True)


def compare(args):
    results = {
        mode: json.loads((args.work / f"{mode}-result.json").read_text())
        for mode in ("map", "stream")
    }
    for key in ("seed", "imgsz", "batch", "epochs", "workers"):
        assert results["map"]["options"][key] == results["stream"]["options"][key], key
    assert results["map"]["options"].get("amp", False) == results["stream"]["options"].get(
        "amp", False
    ), "amp"
    assert (
        results["map"]["initial_checkpoint_sha256"]
        == results["stream"]["initial_checkpoint_sha256"]
    )
    key = "metrics/mAP50-95(B)"
    summary = {
        mode: {
            "best_map50_95_percent": result["metrics"][key] * 100,
            "last_map50_95_percent": result["epochs"][-1]["metrics"][key] * 100,
            "median_steady_train_images_per_second": statistics.median(
                epoch["images_per_second"] for epoch in result["epochs"][1:]
            ),
            "wall_seconds": result["wall_seconds"],
        }
        for mode, result in results.items()
    }
    summary["stream_minus_map_points"] = (
        summary["stream"]["best_map50_95_percent"] - summary["map"]["best_map50_95_percent"]
    )
    summary["within_one_point_observed"] = summary["stream_minus_map_points"] >= -1.0
    summary["steady_train_speed_ratio"] = (
        summary["stream"]["median_steady_train_images_per_second"]
        / summary["map"]["median_steady_train_images_per_second"]
    )
    summary["scope"] = "single-seed subset fine-tuning; not a statistical non-inferiority claim"
    summary["timing_note"] = args.timing_note
    (args.work / "comparison.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=("prepare", "verify", "map", "stream", "compare"))
    p.add_argument("--coco", type=Path, default=Path("/data/datasets/coco2017"))
    p.add_argument("--work", type=Path, required=True)
    p.add_argument("--train-count", type=int, default=8192)
    p.add_argument("--val-count", type=int, default=5000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--imgsz", type=int, default=640)
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--epochs", type=int, default=10)
    p.add_argument("--device", default="0")
    p.add_argument("--model", default="yolo11n.pt")
    p.add_argument("--amp", action="store_true", help="enable FP16 AMP (verified after setup)")
    p.add_argument("--run-name", help="unique output prefix; defaults to map/stream")
    p.add_argument("--source-spec", help="private remote source descriptor from coco_s3_prepare.py")
    p.add_argument("--timing-note", default="Single sequential pair; not a randomized speed trial")
    args = p.parse_args()
    cv2.setNumThreads(1)
    torch.set_num_threads(2)
    if args.mode == "prepare":
        prepare(args)
    elif args.mode == "verify":
        verify(args, json.loads((args.work / "experiment.json").read_text()))
    elif args.mode == "compare":
        compare(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
