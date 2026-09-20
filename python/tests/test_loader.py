from pathlib import Path

import pytest

import shardpack

DATA = Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def dataset(tmp_path_factory: pytest.TempPathFactory) -> shardpack.Dataset:
    config = tmp_path_factory.mktemp("config") / "shardpack.toml"
    config.write_text(
        f'''\
version = 1
default_store = "local"

[stores.local]
type = "filesystem"
root = "{DATA}"
'''
    )
    layout_key = (DATA / "layout-key.txt").read_text().strip()
    return shardpack.Dataset(config, layout_key)


def test_record_loader_prefetches_and_preserves_records(
    dataset: shardpack.Dataset, tmp_path: Path
) -> None:
    cache_dir = tmp_path / "cache"
    loader = dataset.records(
        cache_dir=cache_dir,
        network_concurrency=2,
        network_prefetch_bytes=2 * 1024 * 1024,
        shard_seed=17,
    )
    records = list(loader)

    assert dataset.record_count == 7
    assert dataset.shard_count > 1
    assert {record.sample_id for record in records} == {bytes([value]) * 16 for value in range(7)}
    assert {record.payload for record in records} == {
        f"python-record-{value}".encode() for value in range(7)
    }
    metrics = loader.metrics()
    assert metrics["records_read"] == 7
    assert metrics["shards_completed"] == dataset.shard_count
    assert metrics["prefetched_shards"] == dataset.shard_count
    assert metrics["shards_verified"] == dataset.shard_count
    assert metrics["cache_misses"] == dataset.shard_count
    assert metrics["cache_downloaded_bytes"] == dataset.stored_bytes
    assert records[0].payload is records[0].payload

    cached = dataset.records(cache_dir=cache_dir)
    assert len(list(cached)) == 7
    assert cached.metrics()["cache_hits"] == dataset.shard_count


def test_record_shuffle_is_bounded_deterministic_and_optional(dataset: shardpack.Dataset) -> None:
    unshuffled = [
        record.sample_id for record in dataset.records(shuffle_max_records=0, shuffle_max_bytes=0)
    ]
    shuffled_a = [record.sample_id for record in dataset.records(shuffle_seed=91)]
    shuffled_b = [record.sample_id for record in dataset.records(shuffle_seed=91)]
    shuffled_c = [record.sample_id for record in dataset.records(shuffle_seed=92)]

    assert shuffled_a == shuffled_b
    assert shuffled_a != unshuffled
    assert shuffled_a != shuffled_c
    assert set(shuffled_a) == set(unshuffled)


def test_checkpoint_round_trip_restores_reader_and_shuffle_exactly(
    dataset: shardpack.Dataset, tmp_path: Path
) -> None:
    expected = [record.sample_id for record in dataset.records(shuffle_seed=117)]
    loader = dataset.records(shuffle_seed=117)
    prefix = [next(loader).sample_id for _ in range(3)]
    encoded = loader.checkpoint().to_bytes()
    checkpoint = shardpack.LoaderCheckpoint.from_bytes(encoded)
    suffix = [record.sample_id for record in dataset.resume_records(checkpoint)]
    assert prefix + suffix == expected

    cache_dir = tmp_path / "resume-cache"
    expected_unshuffled = [
        record.sample_id
        for record in dataset.records(
            cache_dir=cache_dir, shuffle_max_records=0, shuffle_max_bytes=0
        )
    ]
    unshuffled = dataset.records(cache_dir=cache_dir, shuffle_max_records=0, shuffle_max_bytes=0)
    prefix = [next(unshuffled).sample_id for _ in range(2)]
    resumed = dataset.resume_records(unshuffled.checkpoint(), cache_dir=cache_dir)
    suffix = [record.sample_id for record in resumed]
    assert prefix + suffix == expected_unshuffled
    assert resumed.metrics()["cache_hits"] >= 1

    with pytest.raises(ValueError, match="invalid loader checkpoint"):
        shardpack.LoaderCheckpoint.from_bytes(b"not-a-checkpoint")


def test_batch_loader_and_distributed_partition(dataset: shardpack.Dataset) -> None:
    batches = list(dataset.batches(3, shard_seed=23))
    assert [len(batch) for batch in batches] == [3, 3, 1]

    drop_last = list(dataset.batches(3, drop_last=True, shard_seed=23))
    assert [len(batch) for batch in drop_last] == [3, 3]

    expected = [record.sample_id for batch in batches for record in batch]
    loader = dataset.batches(3, shard_seed=23)
    first = next(loader)
    resumed = dataset.resume_records(loader.checkpoint())
    assert [record.sample_id for record in first] + [
        record.sample_id for record in resumed
    ] == expected

    rank_zero = list(dataset.records(shard_seed=23, rank=0, world_size=2))
    rank_one = list(dataset.records(shard_seed=23, rank=1, world_size=2))
    zero_ids = {record.sample_id for record in rank_zero}
    one_ids = {record.sample_id for record in rank_one}
    assert zero_ids.isdisjoint(one_ids)
    assert zero_ids | one_ids == {bytes([value]) * 16 for value in range(7)}


def test_invalid_limits_are_python_errors(dataset: shardpack.Dataset) -> None:
    with pytest.raises(ValueError, match="batch_size"):
        dataset.batches(0)
    with pytest.raises(RuntimeError, match="network_concurrency"):
        dataset.records(network_concurrency=0)
    with pytest.raises(ValueError, match="both be zero or non-zero"):
        dataset.records(shuffle_max_records=0)


def test_encrypted_layout_requires_matching_external_key(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    config = tmp_path_factory.mktemp("encrypted-config") / "shardpack.toml"
    config.write_text(
        f'''\
version = 1
default_store = "local"

[stores.local]
type = "filesystem"
root = "{DATA}"
'''
    )
    layout_key = (DATA / "encrypted-layout-key.txt").read_text().strip()
    dataset = shardpack.Dataset(config, layout_key)

    with pytest.raises(ValueError, match="provided together"):
        dataset.records(encryption_key_id="test://python-key")
    with pytest.raises(ValueError, match="exactly 32 bytes"):
        dataset.records(encryption_key_id="test://python-key", encryption_key=b"short")

    records = list(
        dataset.records(
            encryption_key_id="test://python-key",
            encryption_key=bytes([0x77]) * 32,
        )
    )
    assert len(records) == 7

    loader = dataset.records(
        encryption_key_id="test://python-key",
        encryption_key=bytes([0x77]) * 32,
    )
    prefix = [next(loader).sample_id for _ in range(2)]
    checkpoint = loader.checkpoint()
    with pytest.raises(ValueError, match="requires encryption_key"):
        checkpoint.to_bytes()
    checkpoint_key = bytes([0x35]) * 32
    checkpoint_bytes = checkpoint.to_bytes(checkpoint_key)
    checkpoint = shardpack.LoaderCheckpoint.from_bytes(checkpoint_bytes, checkpoint_key)
    tampered = bytearray(checkpoint_bytes)
    tampered[-1] ^= 1
    with pytest.raises(ValueError, match="authentication failed"):
        shardpack.LoaderCheckpoint.from_bytes(bytes(tampered), checkpoint_key)
    resumed = dataset.resume_records(
        checkpoint,
        encryption_key_id="test://python-key",
        encryption_key=bytes([0x77]) * 32,
    )
    assert len(prefix) + len(list(resumed)) == 7
