import importlib
import shutil
from pathlib import Path

import pytest

from shardpack import Record

torch = pytest.importorskip("torch")
ShardPackDataset = importlib.import_module("shardpack.pytorch").ShardPackDataset

DATA = Path(__file__).parent / "data"


def decode_sample_id(record: Record) -> int:
    return record.sample_id[0]


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
