"""Optional real trainer test; no pretrained weights or dataset downloads."""

import importlib.util
import io
import sys
from array import array
from pathlib import Path

import pytest

torch = pytest.importorskip("torch")
np = pytest.importorskip("numpy")
pytest.importorskip("ultralytics")

from shardpack import DatasetWriter, ShardPackSource

spec = importlib.util.spec_from_file_location(
    "yolo_streaming", Path(__file__).parents[1] / "examples" / "yolo_streaming.py"
)
adapter = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = adapter
spec.loader.exec_module(adapter)


def decode(record):
    with np.load(io.BytesIO(record.payload), allow_pickle=False) as data:
        image = torch.from_numpy(data["image"])
        cls = torch.from_numpy(data["cls"])
        boxes = torch.from_numpy(data["boxes"])
    return {
        "img": image,
        "cls": cls,
        "bboxes": boxes,
        "im_file": record.sample_id.hex() + ".jpg",
        "ori_shape": (64, 64),
        "ratio_pad": ((1.0, 1.0), (0, 0)),
    }


def make_source(tmp_path, split, count):
    config = {
        "default_store": "local",
        "stores": {"local": {"type": "filesystem", "root": tmp_path / "objects"}},
    }
    writer = DatasetWriter(
        bytes([count]) * 32,
        tmp_path / split,
        config=config,
        target_block_bytes=32 * 1024,
        target_shard_bytes=64 * 1024,
    )
    payloads = []
    for index in range(count):
        buffer = io.BytesIO()
        # Exercise both empty annotations and multiple boxes in the same batch.
        n = index % 3
        np.savez(
            buffer,
            image=np.full((3, 64, 64), index * 20, dtype=np.uint8),
            cls=np.zeros((n, 1), dtype=np.float32),
            boxes=np.tile(np.array([[0.5, 0.5, 0.25, 0.25]], dtype=np.float32), (n, 1)),
        )
        payloads.append(buffer.getvalue())
    offsets = array("Q", [0])
    for payload in payloads:
        offsets.append(offsets[-1] + len(payload))
    writer.write_batch(
        b"".join(payloads), offsets, b"".join(i.to_bytes(16, "little") for i in range(count))
    )
    return ShardPackSource(
        writer.finish(),
        config=config,
        rank=0,
        world_size=1,
        shuffle_max_records=3 if split == "train" else 0,
        shuffle_max_bytes=128 * 1024 if split == "train" else 0,
    )


def test_real_yolo_training_and_validation(tmp_path):
    old_threads = torch.get_num_threads()
    torch.set_num_threads(2)
    try:
        trainer = adapter.ShardPackDetectionTrainer(
            train_source=make_source(tmp_path, "train", 5),
            val_source=make_source(tmp_path, "val", 3),
            train_decode=decode,
            val_decode=decode,
            names=["object"],
            decode_workers=2,
            overrides={
                "model": "yolo11n.yaml",
                "epochs": 2,
                "batch": 2,
                "nbs": 2,
                "warmup_epochs": 0,
                "imgsz": 64,
                "device": "cpu",
                "amp": False,
                "pretrained": False,
                "project": str(tmp_path / "runs"),
                "name": "stream",
                "exist_ok": True,
            },
        )
        counts = []
        initial_weights = []
        trainer.add_callback(
            "on_train_start",
            lambda t: initial_weights.append(next(t.model.parameters()).detach().clone()),
        )
        trainer.add_callback(
            "on_train_epoch_end",
            lambda t: counts.append(t.train_loader.stats()["source"]["records_read"]),
        )
        trainer.train()
        assert counts == [5, 5]
        assert not torch.equal(initial_weights[0], next(trainer.model.parameters()).detach())
        assert not hasattr(trainer.train_loader.dataset, "__getitem__")
        assert len(trainer.train_loader) == 3  # retain the final partial batch
        assert trainer.test_loader.stats()["source"]["records_read"] == 3
        assert trainer.best.exists()
        assert all(np.isfinite(value) for value in trainer.metrics.values())
    finally:
        torch.set_num_threads(old_threads)


def test_collate_empty_boxes():
    sample = {
        "img": torch.zeros((3, 64, 64), dtype=torch.uint8),
        "cls": torch.empty((0, 1)),
        "bboxes": torch.empty((0, 4)),
        "im_file": "empty.jpg",
        "ori_shape": (64, 64),
        "ratio_pad": ((1.0, 1.0), (0, 0)),
    }
    batch = adapter.detection_collate([sample, sample])
    assert batch["bboxes"].shape == (0, 4)
    assert batch["cls"].shape == (0, 1)
    assert batch["batch_idx"].shape == (0,)


@pytest.mark.parametrize(
    "overrides",
    [{"batch": -1}, {"device": "0,1"}, {"mosaic": 1.0}, {"resume": True}, {"plots": True}],
)
def test_unsupported_modes_fail_before_training(overrides):
    with pytest.raises(ValueError):
        adapter.ShardPackDetectionTrainer(
            train_source=None,
            val_source=None,
            train_decode=decode,
            val_decode=decode,
            names=["object"],
            overrides=overrides,
        )
