"""Check the paired benchmark's geometry and serialization before GPU runs."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

np = pytest.importorskip("numpy")
cv2 = pytest.importorskip("cv2")
torch = pytest.importorskip("torch")
pytest.importorskip("ultralytics")

spec = importlib.util.spec_from_file_location(
    "coco_yolo_alignment", Path(__file__).parents[1] / "benchmarks" / "coco_yolo_alignment.py"
)
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def test_letterbox_non_square_geometry_and_payload_equivalence():
    # 100x200 -> 320x640, padded by 160 pixels vertically.
    image = np.zeros((100, 200, 3), dtype=np.uint8)
    image[:] = (10, 20, 30)
    ok, encoded = cv2.imencode(".png", image)
    assert ok
    row = {"id": 1, "height": 100, "width": 200, "boxes": [[50, 25, 100, 50]], "classes": [4]}
    decode = benchmark.Decode(640)
    direct = decode.image(row, encoded)
    packed = decode(SimpleNamespace(payload=memoryview(benchmark.payload(row, encoded.tobytes()))))
    assert direct["img"].shape == (3, 640, 640)
    assert direct["img"][:, 320, 320].tolist() == [30, 20, 10]  # RGB
    assert direct["img"][:, 0, 0].tolist() == [114, 114, 114]
    torch.testing.assert_close(direct["bboxes"], torch.tensor([[0.5, 0.5, 0.5, 0.25]]))
    assert direct["ratio_pad"] == ((3.2, 3.2), (0, 160))
    for key in ("img", "bboxes", "cls"):
        assert torch.equal(direct[key], packed[key])


def test_empty_annotations():
    ok, encoded = cv2.imencode(".png", np.zeros((31, 57, 3), dtype=np.uint8))
    assert ok
    row = {"id": 2, "height": 31, "width": 57, "boxes": [], "classes": []}
    decoded = benchmark.Decode(64).image(row, encoded)
    assert decoded["img"].shape == (3, 64, 64)
    assert decoded["bboxes"].shape == (0, 4)
    assert decoded["cls"].shape == (0, 1)


def test_s3_experiment_config_is_scoped_without_serializing_credentials(tmp_path):
    helper_spec = importlib.util.spec_from_file_location(
        "coco_s3_prepare", Path(__file__).parents[1] / "benchmarks" / "coco_s3_prepare.py"
    )
    helper = importlib.util.module_from_spec(helper_spec)
    helper_spec.loader.exec_module(helper)
    config_path = tmp_path / "config.toml"
    config_path.write_text('[stores.internal]\ntype="s3"\nbucket="test"\nroot="base/"\n')
    spec = {
        "config_path": str(config_path),
        "store": "internal",
        "root_suffix": "experiments/coco-s3-test",
    }
    result = helper.scoped_config(spec)
    assert result["stores"]["internal"]["root"] == "base/experiments/coco-s3-test"
    assert 'root="base/"' in config_path.read_text()
    for suffix in ("", "existing-data", "experiments/coco-s3-test/../existing-data"):
        with pytest.raises(ValueError):
            helper.scoped_config(spec | {"root_suffix": suffix})
