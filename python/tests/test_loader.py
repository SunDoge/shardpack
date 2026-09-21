from pathlib import Path

import numpy as np
import pytest

import shardpack

DATA = Path(__file__).parent / "data"


def test_buffer_writer_materializes_numpy_batches(tmp_path: Path) -> None:
    object_root = tmp_path / "objects"
    staging = tmp_path / "staging"
    config = tmp_path / "shardpack.toml"
    config.write_text(
        f'''\
version = 1
default_store = "local"

[stores.local]
type = "filesystem"
root = "{object_root}"
'''
    )
    payloads = [b"alpha", b"", b"charlie-data"]
    payload_data = np.frombuffer(b"".join(payloads), dtype=np.uint8).copy()
    offsets = np.array([0, 5, 5, 17], dtype=np.uint64)
    sample_ids = np.stack([np.full(16, value, dtype=np.uint8) for value in range(len(payloads))])
    flags = np.array([3, 0, 9], dtype=np.uint32)

    writer = shardpack.DatasetWriter(
        config,
        bytes([0x61]) * 32,
        staging,
        target_shard_bytes=64,
        target_block_bytes=32,
        queue_batches=1,
    )
    assert writer.write_batch(payload_data, offsets, sample_ids, flags) == 3
    payload_data.fill(0)  # Rust owns the admitted batch; later mutation is harmless.
    assert (
        writer.write_batch(
            memoryview(b"delta"),
            np.array([0, 5], dtype=np.uint64),
            np.full((1, 16), 7, dtype=np.uint8),
        )
        == 1
    )
    layout_key = writer.finish()
    assert writer.finish() == layout_key

    dataset = shardpack.Dataset(config, layout_key)
    records = list(dataset.records())
    assert {record.sample_id: (record.flags, record.payload) for record in records} == {
        bytes([0]) * 16: (3, b"alpha"),
        bytes([1]) * 16: (0, b""),
        bytes([2]) * 16: (9, b"charlie-data"),
        bytes([7]) * 16: (0, b"delta"),
    }


def test_buffer_writer_rejects_invalid_packed_batches(tmp_path: Path) -> None:
    config = tmp_path / "shardpack.toml"
    config.write_text(
        f'''\
version = 1
default_store = "local"

[stores.local]
type = "filesystem"
root = "{tmp_path / "objects"}"
'''
    )
    writer = shardpack.DatasetWriter(
        config,
        bytes([0x62]) * 32,
        tmp_path / "staging",
        max_batch_records=1,
        max_batch_bytes=8,
    )
    ids = np.zeros((1, 16), dtype=np.uint8)
    with pytest.raises(ValueError, match="max_batch_bytes"):
        writer.write_batch(np.zeros(9, dtype=np.uint8), np.array([0, 9], dtype=np.uint64), ids)
    with pytest.raises(ValueError, match="C-contiguous"):
        writer.write_batch(
            np.arange(8, dtype=np.uint8)[::2], np.array([0, 4], dtype=np.uint64), ids
        )
    with pytest.raises(ValueError, match="offsets"):
        writer.write_batch(np.zeros(4, dtype=np.uint8), np.array([0, 5], dtype=np.uint64), ids)
    with pytest.raises(ValueError, match="sample_ids"):
        writer.write_batch(
            np.zeros(4, dtype=np.uint8),
            np.array([0, 4], dtype=np.uint64),
            np.zeros((1, 15), dtype=np.uint8),
        )
    with pytest.raises(ValueError, match="max_batch_records"):
        writer.write_batch(
            np.zeros(4, dtype=np.uint8),
            np.array([0, 2, 4], dtype=np.uint64),
            np.zeros((2, 16), dtype=np.uint8),
        )
    writer.finish()


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


def test_simple_record_reader_preserves_records(dataset: shardpack.Dataset) -> None:
    records = list(dataset.records())

    assert dataset.record_count == 7
    assert dataset.shard_count > 1
    assert {record.sample_id for record in records} == {bytes([value]) * 16 for value in range(7)}
    assert {record.payload for record in records} == {
        f"python-record-{value}".encode() for value in range(7)
    }
    assert records[0].payload is records[0].payload


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
