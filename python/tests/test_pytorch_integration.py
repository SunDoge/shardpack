import shutil
from array import array
from pathlib import Path

import pytest

from shardpack import DatasetWriter, Record, ShardPackSource, ThreadedLoader

pyfory = pytest.importorskip("pyfory")
torch = pytest.importorskip("torch")
ThreadedDataLoader = pytest.importorskip("shardpack.pytorch").ThreadedDataLoader
FORY = pyfory.Fory()
DATA = Path(__file__).parent / "data"


def decode_sample_id(record: Record) -> int:
    return record.sample_id[0]


def decode_fory(record: Record) -> dict[str, object]:
    buffer = pyfory.Buffer(record.payload)
    assert not buffer.own_data()
    return FORY.deserialize(buffer)


def write_config(path: Path, root: Path) -> None:
    path.write_text(
        f'''\
version = 1
default_store = "local"

[stores.local]
type = "filesystem"
root = "{root}"
'''
    )


def test_fory_writer_to_threaded_pytorch_round_trip(tmp_path: Path) -> None:
    objects = tmp_path / "fory-objects"
    config = tmp_path / "fory.toml"
    write_config(config, objects)
    samples = [
        {"index": index, "label": index % 3, "text": f"sample-{index}"} for index in range(17)
    ]
    encoded = [FORY.serialize(sample) for sample in samples]
    offsets = array("Q", [0])
    for payload in encoded:
        offsets.append(offsets[-1] + len(payload))

    writer = DatasetWriter(
        bytes([0xF0]) * 32,
        tmp_path / "fory-staging",
        config_path=config,
        target_shard_bytes=256,
        target_block_bytes=128,
    )
    writer.write_batch(
        memoryview(b"".join(encoded)),
        offsets,
        memoryview(b"".join(index.to_bytes(16, "little") for index in range(len(samples)))),
        array("I", [0] * len(samples)),
    )
    layout = writer.finish()

    loader = ThreadedDataLoader(
        ShardPackSource(layout, config_path=config, cache_dir=tmp_path / "cache"),
        decode=decode_fory,
        batch_size=4,
        decode_workers=2,
        enable_stats=True,
    )
    decoded = {
        int(index): (int(label), text)
        for batch in loader
        for index, label, text in zip(batch["index"], batch["label"], batch["text"])
    }
    assert decoded == {sample["index"]: (sample["label"], sample["text"]) for sample in samples}
    stats = loader.stats()
    assert stats["batches_delivered"] == 5
    assert stats["source"]["records_read"] == 17
    assert stats["timings"]["decode"]["items"] == 17
    assert stats["status"] == "completed"


def test_rust_epoch_and_pool_shuffle_advance_without_iterable_dataset(tmp_path: Path) -> None:
    objects = tmp_path / "objects"
    shutil.copytree(DATA, objects)
    config = tmp_path / "shardpack.toml"
    write_config(config, objects)
    layout = (objects / "layout-key.txt").read_text().strip()
    source = ShardPackSource(
        layout,
        config_path=config,
        shuffle_max_records=3,
        shuffle_max_bytes=1024 * 1024,
        shuffle_seed=71,
    )
    loader = ThreadedLoader(source, decode=decode_sample_id, batch_size=1, decode_workers=2)

    first = [value for batch in loader for value in batch]
    second = [value for batch in loader for value in batch]

    assert set(first) == set(range(7))
    assert set(second) == set(range(7))
    assert first != second


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires a CUDA GPU")
def test_pinned_batches_transfer_non_blocking_on_cuda_stream() -> None:
    saw_pinned: list[bool] = []
    stream = torch.cuda.Stream()

    def move_to_gpu(batch: torch.Tensor) -> torch.Tensor:
        saw_pinned.append(batch.is_pinned())
        with torch.cuda.stream(stream):
            return batch.to("cuda", non_blocking=True)

    loader = ThreadedDataLoader(
        range(32),
        decode=lambda value: torch.tensor([value], dtype=torch.int64),
        batch_size=8,
        decode_workers=2,
        pin_memory=True,
        consumer_fn=move_to_gpu,
    )
    batches = list(loader)
    torch.cuda.current_stream().wait_stream(stream)
    values = torch.cat(batches).cpu().flatten().tolist()

    assert all(saw_pinned)
    assert values == list(range(32))
