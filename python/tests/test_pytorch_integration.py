import importlib
import shutil
from array import array
from pathlib import Path

import pytest

from shardpack import DatasetWriter, Record

torch = pytest.importorskip("torch")
pyfory = pytest.importorskip("pyfory")
ShardPackDataset = importlib.import_module("shardpack.pytorch").ShardPackDataset
FORY = pyfory.Fory()

DATA = Path(__file__).parent / "data"


def decode_sample_id(record: Record) -> int:
    assert isinstance(record.payload, memoryview)
    assert record.payload.readonly
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


def collect(source: object) -> list[int]:
    loader = torch.utils.data.DataLoader(
        source,
        batch_size=2,
        num_workers=2,
        prefetch_factor=2,
        multiprocessing_context="spawn",
    )
    return [int(value) for batch in loader for value in batch]


def test_real_dataloader_workers_manual_distributed_partition_and_hot_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    objects = tmp_path / "objects"
    shutil.copytree(DATA, objects)
    config = tmp_path / "shardpack.toml"
    write_config(config, objects)
    layout = (objects / "layout-key.txt").read_text().strip()
    cache = tmp_path / "cache"

    common = {
        "config": config,
        "layout": layout,
        "decode": decode_sample_id,
        "cache_dir": cache,
        "network_concurrency": 2,
        "network_prefetch_bytes": 1024 * 1024,
        "shard_seed": 41,
    }
    monkeypatch.setenv("RANK", "7")
    monkeypatch.setenv("WORLD_SIZE", "8")
    rank_zero = collect(ShardPackDataset(**common, rank=0, world_size=2))
    rank_one = collect(ShardPackDataset(**common, rank=1, world_size=2))

    assert set(rank_zero).isdisjoint(rank_one)
    assert set(rank_zero) | set(rank_one) == set(range(7))
    assert len(rank_zero) + len(rank_one) == 7

    # The cold distributed pass populated every assigned shard. Removing the
    # source proves that a new pair of real DataLoader workers can train from
    # the shared verified cache alone.
    shutil.rmtree(objects / "shards")
    hot = collect(ShardPackDataset(**common, rank=0, world_size=1, epoch=1))
    assert set(hot) == set(range(7))
    assert len(hot) == 7


def test_fory_writer_to_real_dataloader_round_trip(tmp_path: Path) -> None:
    objects = tmp_path / "fory-objects"
    config = tmp_path / "fory.toml"
    write_config(config, objects)
    samples = [
        {"index": index, "label": index % 3, "text": f"sample-{index}"} for index in range(17)
    ]
    encoded = [FORY.serialize(sample) for sample in samples]
    lengths = [len(payload) for payload in encoded]
    offsets = array("Q", [0])
    for length in lengths:
        offsets.append(offsets[-1] + length)

    writer = DatasetWriter(
        config,
        bytes([0xF0]) * 32,
        tmp_path / "fory-staging",
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

    source = ShardPackDataset(
        config,
        layout,
        decode=decode_fory,
        cache_dir=tmp_path / "fory-cache",
        network_concurrency=2,
        network_prefetch_bytes=1024 * 1024,
    )
    loader = torch.utils.data.DataLoader(
        source,
        batch_size=4,
        num_workers=2,
        multiprocessing_context="spawn",
    )
    decoded = {
        int(index): (int(label), text)
        for batch in loader
        for index, label, text in zip(batch["index"], batch["label"], batch["text"])
    }
    assert decoded == {sample["index"]: (sample["label"], sample["text"]) for sample in samples}


def test_bounded_pool_shuffle_is_complete_and_deterministic(tmp_path: Path) -> None:
    objects = tmp_path / "objects"
    shutil.copytree(DATA, objects)
    config = tmp_path / "shardpack.toml"
    write_config(config, objects)
    layout = (objects / "layout-key.txt").read_text().strip()

    def shuffled(seed: int) -> list[int]:
        source = ShardPackDataset(
            config,
            layout,
            decode=decode_sample_id,
            shuffle_max_records=3,
            shuffle_max_bytes=1024 * 1024,
            shuffle_seed=seed,
        )
        return [int(value) for value in torch.utils.data.DataLoader(source, batch_size=None)]

    first = shuffled(91)
    assert first == shuffled(91)
    assert first != shuffled(92)
    assert set(first) == set(range(7))
