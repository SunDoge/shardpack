import threading
from pathlib import Path

import numpy as np
import pytest

import shardpack

DATA = Path(__file__).parent / "data"


def test_node_affinity_coverage_cache_ownership_and_validation(monkeypatch, tmp_path):
    config = {"stores": {"local": {"type": "filesystem", "root": DATA}}}
    key = (DATA / "layout-key.txt").read_text().strip()
    dataset = shardpack.Dataset(key, config=config)
    expected = {r.sample_id for r in dataset.records()}
    monkeypatch.setenv("RANK", "999")
    monkeypatch.setenv("WORLD_SIZE", "1000")
    previous = None
    for epoch in (0, 1):
        ownership = {}
        all_ids = set()
        for node in ("a", "b"):
            local_ids = set()
            for rank in (0, 1):
                affinity = shardpack.NodeAffinity(
                    node_id=node,
                    node_ids=["a", "b"] if epoch == 0 else ["b", "a"],
                    local_rank=rank,
                    local_world_size=2,
                    seed=7,
                )
                source = shardpack.ShardPackSource(
                    key,
                    config=config,
                    affinity=affinity,
                    epoch=epoch,
                    shard_seed=42,
                    cache_dir=tmp_path / node,
                )
                ids = {r.sample_id for r in source}
                if epoch == 1:
                    assert source.stats()["cache_downloaded_bytes"] == 0
                    if ids:
                        assert source.stats()["cache_hits"] > 0
                assert local_ids.isdisjoint(ids)
                local_ids.update(ids)
                with pytest.raises(ValueError, match="replaces"):
                    dataset.records(affinity=affinity, world_size=2)
                with pytest.raises(ValueError, match="replaces"):
                    shardpack.ShardPackSource(
                        key, config=config, affinity=affinity, rank=0, world_size=4
                    )
            assert all_ids.isdisjoint(local_ids)
            all_ids.update(local_ids)
            ownership[node] = local_ids
        assert all_ids == expected
        if previous is not None:
            assert ownership == previous
        previous = ownership
    with pytest.raises(RuntimeError, match="duplicate"):
        shardpack.NodeAffinity(node_id="a", node_ids=["a", "a"], local_rank=0, local_world_size=1)


def test_affinity_epoch_validation_uses_affinity_topology():
    dataset = shardpack.Dataset(
        (DATA / "layout-key.txt").read_text().strip(),
        config={"stores": {"local": {"type": "filesystem", "root": DATA}}},
    )
    one = shardpack.NodeAffinity(node_id="a", node_ids=["a"], local_rank=0, local_world_size=1)
    assert dataset.validate_epoch(shard_seed=42, world_size=1, batch_size=2, affinity=one) == [7]
    with pytest.raises(ValueError, match="differs"):
        dataset.validate_epoch(shard_seed=42, world_size=2, batch_size=2, affinity=one)


def test_trusted_identity_and_source_resolution():
    config = {"stores": {"local": {"type": "filesystem", "root": DATA}}}
    layout = (DATA / "layout-key.txt").read_text().strip()
    trusted = shardpack.Dataset(layout, config=config)
    source = shardpack.ShardPackSource(
        layout,
        config=config,
        expected_layout_id=trusted.layout_id,
        expected_snapshot_id=trusted.snapshot_id,
    )
    assert source.resolve() is source.resolve()
    # Resolution pins the object, not an address to re-fetch each epoch.
    source.layout = "missing.pb"
    assert len(list(source)) == 7
    assert len(list(source)) == 7
    for snapshot, physical in (
        (trusted.snapshot_id, bytes(32)),
        (bytes(32), trusted.layout_id),
    ):
        with pytest.raises(RuntimeError, match="trusted dataset identity"):
            shardpack.Dataset(
                layout, config=config, expected_snapshot_id=snapshot, expected_layout_id=physical
            )
    with pytest.raises(ValueError, match="together"):
        shardpack.Dataset(layout, config=config, expected_layout_id=trusted.layout_id)


def test_cancellation_handle_is_cross_thread_and_terminal():
    from concurrent.futures import ThreadPoolExecutor

    dataset = shardpack.Dataset(
        (DATA / "layout-key.txt").read_text().strip(),
        config={"stores": {"local": {"type": "filesystem", "root": DATA}}},
    )
    for shuffle in (False, True):
        reader = dataset.records(
            shuffle_max_records=3 if shuffle else 0,
            shuffle_max_bytes=4096 if shuffle else 0,
        )
        handle = reader.cancellation_handle()
        next(reader)
        with ThreadPoolExecutor(1) as executor:
            executor.submit(handle.cancel).result()
        with pytest.raises(RuntimeError, match="cancelled"):
            next(reader)
        with pytest.raises(RuntimeError, match="cancelled"):
            next(reader)
        del reader
        handle.cancel()


def test_distributed_loader_rejects_empty_rank_before_decode():
    source = shardpack.ShardPackSource(
        (DATA / "layout-key.txt").read_text().strip(),
        config={"stores": {"local": {"type": "filesystem", "root": DATA}}},
        rank=0,
        world_size=100,
    )
    decoded = []
    loader = shardpack.ThreadedLoader(
        source,
        decode=lambda record: decoded.append(record),
        batch_size=2,
    )
    with pytest.raises(RuntimeError, match="unequal or zero steps"):
        list(loader)
    assert decoded == []


@pytest.mark.parametrize("wrapped", [False, True])
def test_cancel_interrupts_pending_http_read(monkeypatch, wrapped):
    from concurrent.futures import ThreadPoolExecutor
    from functools import partial
    from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
    from queue import Queue

    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    entered = threading.Event()
    release = threading.Event()
    handles = Queue()

    class Handler(SimpleHTTPRequestHandler):
        def do_GET(self):
            if self.path.endswith(".spk"):
                entered.set()
                release.wait(10)
                self.close_connection = True
                return
            super().do_GET()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(Handler, directory=str(DATA)))
    thread = threading.Thread(target=server.serve_forever)
    thread.start()

    def read():
        if wrapped:
            source = shardpack.ShardPackSource(
                (DATA / "layout-key.txt").read_text().strip(),
                config={
                    "stores": {
                        "local": {
                            "type": "http",
                            "base_url": f"http://127.0.0.1:{server.server_port}/",
                        }
                    }
                },
            )
            groups = shardpack.SamplePoolSource(source)
            handles.put(groups)
            iterator = iter(
                shardpack.ThreadedLoader(
                    groups,
                    decode=lambda group: group,
                    batch_size=1,
                )
            )
            try:
                return next(iterator)
            finally:
                iterator.close()
        dataset = shardpack.Dataset(
            (DATA / "layout-key.txt").read_text().strip(),
            config={
                "stores": {
                    "local": {
                        "type": "http",
                        "base_url": f"http://127.0.0.1:{server.server_port}/",
                    }
                }
            },
        )
        reader = dataset.records()
        handles.put(reader.cancellation_handle())
        try:
            return next(reader)
        finally:
            del reader

    executor = ThreadPoolExecutor(1)
    try:
        future = executor.submit(read)
        handle = handles.get(timeout=5)
        assert entered.wait(5)
        handle.cancel()
        with pytest.raises(RuntimeError, match="cancelled"):
            future.result(timeout=3)
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join()
        executor.shutdown(wait=True)


@pytest.mark.parametrize("enabled", [False, True])
def test_cache_plan_is_read_only_and_reports_capacity(tmp_path, monkeypatch, enabled):
    from types import SimpleNamespace

    monkeypatch.setattr("shardpack.loader.shutil.disk_usage", lambda path: SimpleNamespace(free=1))
    cache = tmp_path / "missing" / "cache"
    source = shardpack.ShardPackSource(
        (DATA / "layout-key.txt").read_text().strip(),
        config={"stores": {"local": {"type": "filesystem", "root": DATA}}},
        cache_dir=cache if enabled else None,
        cache_max_bytes=1,
    )
    plan = source.cache_plan()
    assert plan["scope"] == "whole_layout"
    assert plan["stored_bytes"] > 1
    assert plan["cache_enabled"] == enabled
    assert not plan["budget_covers_stored_bytes"]
    assert plan["free_disk_covers_stored_bytes"] is (False if enabled else None)
    assert not cache.exists()
    assert source.stats() == {}
    source.cache_max_bytes = plan["stored_bytes"]
    assert source.cache_plan()["budget_covers_stored_bytes"]


def test_live_source_metrics_are_safe_from_decode_and_consumer_threads(tmp_path):
    source = shardpack.ShardPackSource(
        (DATA / "layout-key.txt").read_text().strip(),
        config={
            "default_store": "local",
            "stores": {"local": {"type": "filesystem", "root": DATA}},
        },
        cache_dir=tmp_path / "cache",
    )
    snapshots = []

    def decode(record):
        # One-record prefetch keeps the native iterator alive on the producer.
        assert source._metrics_handle is not None
        snapshot = source.stats()
        assert snapshot["records_read"] >= 1
        assert "cache_misses" in snapshot
        snapshots.append(snapshot)
        return record.sample_id

    loader = shardpack.ThreadedLoader(
        source,
        decode=decode,
        decode_workers=1,
        decode_prefetch=1,
        decode_task_records=1,
        batch_size=1,
        prefetch_batches=1,
        enable_stats=True,
    )
    count = 0
    for _ in loader:
        assert loader.stats()["source"]["records_read"] >= 1
        count += 1
    assert count == len(snapshots) == 7
    assert source.stats()["records_read"] == 7


def test_native_metrics_handle_outlives_reader_and_is_sendable():
    from concurrent.futures import ThreadPoolExecutor

    dataset = shardpack.Dataset(
        (DATA / "layout-key.txt").read_text().strip(),
        config={"stores": {"local": {"type": "filesystem", "root": DATA}}},
    )
    reader = dataset.records()
    handle = reader.metrics_handle()
    with ThreadPoolExecutor(1) as pool:
        assert pool.submit(handle.snapshot).result()["records_read"] == 0
        assert len(list(reader)) == 7
        del reader
        assert pool.submit(handle.snapshot).result()["records_read"] == 7


def test_config_and_config_path_are_mutually_exclusive(tmp_path: Path) -> None:
    config = {"stores": {"local": {"type": "filesystem", "root": tmp_path}}}
    with pytest.raises(ValueError, match="exactly one"):
        shardpack.Dataset("layout.pb")
    with pytest.raises(ValueError, match="exactly one"):
        shardpack.Dataset("layout.pb", config=config, config_path=tmp_path / "config.toml")


def test_framework_neutral_threaded_loader_batches_and_consumes_on_caller_thread() -> None:
    caller_thread = threading.get_ident()
    decode_threads: set[int] = set()
    consumer_threads: list[int] = []

    def decode(value: int) -> int:
        decode_threads.add(threading.get_ident())
        return value * 2

    def consume(batch: list[int]) -> tuple[int, ...]:
        consumer_threads.append(threading.get_ident())
        return tuple(batch)

    loader = shardpack.ThreadedLoader(
        range(7),
        decode=decode,
        batch_size=3,
        decode_workers=2,
        decode_prefetch=4,
        decode_task_records=2,
        prefetch_batches=1,
        consumer_fn=consume,
    )

    assert list(loader) == [(0, 2, 4), (6, 8, 10), (12,)]
    assert decode_threads
    assert caller_thread not in decode_threads
    assert consumer_threads == [caller_thread] * 3


def test_buffer_writer_materializes_numpy_batches(tmp_path: Path) -> None:
    object_root = tmp_path / "objects"
    staging = tmp_path / "staging"
    config = {
        "version": 1,
        "default_store": "local",
        "stores": {"local": {"type": "filesystem", "root": object_root}},
    }
    payloads = [b"alpha", b"", b"charlie-data"]
    payload_data = np.frombuffer(b"".join(payloads), dtype=np.uint8).copy()
    offsets = np.array([0, 5, 5, 17], dtype=np.uint64)
    sample_ids = np.stack([np.full(16, value, dtype=np.uint8) for value in range(len(payloads))])
    flags = np.array([3, 0, 9], dtype=np.uint32)

    writer = shardpack.DatasetWriter(
        bytes([0x61]) * 32,
        staging,
        config=config,
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

    dataset = shardpack.Dataset(layout_key, config=config)
    records = list(dataset.records())
    assert {record.sample_id: (record.flags, bytes(record.payload)) for record in records} == {
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
        bytes([0x62]) * 32,
        tmp_path / "staging",
        config_path=config,
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
    return shardpack.Dataset(layout_key, config_path=config)


@pytest.mark.parametrize("codec", ["none", "zstd"])
@pytest.mark.parametrize("shuffle_records", [0, 4])
def test_memoryview_survives_block_reuse_and_loader_drop(
    tmp_path: Path, codec: str, shuffle_records: int
) -> None:
    config = {
        "default_store": "local",
        "stores": {
            "local": {
                "type": "filesystem",
                "root": tmp_path / "objects",
            }
        },
    }
    writer = shardpack.DatasetWriter(
        bytes([0x71]) * 32,
        tmp_path / "staging",
        config=config,
        target_shard_bytes=1024 * 1024,
        target_block_bytes=1024,
        block_codec=codec,
    )
    samples = [bytes([i]) * 1024 for i in range(32)]
    writer.write_batch(
        b"".join(samples),
        np.arange(33, dtype=np.uint64) * 1024,
        np.repeat(np.arange(32, dtype=np.uint8)[:, None], 16, axis=1),
    )
    source = shardpack.Dataset(writer.finish(), config=config)
    loader = source.records(
        shuffle_max_records=shuffle_records, shuffle_max_bytes=4096 if shuffle_records else 0
    )
    record = next(loader)
    view = record.payload[1:]
    expected = samples[record.sample_id[0]][1:]
    del record
    count = 1
    for record in loader:
        assert bytes(record.payload) == samples[record.sample_id[0]]
        count += 1
    assert count == 32
    del record, loader, source
    assert view.readonly
    assert bytes(view) == expected


def test_simple_record_reader_preserves_records(dataset: shardpack.Dataset) -> None:
    loader = dataset.records()
    records = list(loader)

    assert dataset.record_count == 7
    assert dataset.shard_count > 1
    assert {record.sample_id for record in records} == {bytes([value]) * 16 for value in range(7)}
    assert {bytes(record.payload) for record in records} == {
        f"python-record-{value}".encode() for value in range(7)
    }
    payload = records[0].payload
    assert isinstance(payload, memoryview)
    assert payload.readonly
    assert payload.format == "B"
    assert payload.c_contiguous
    assert payload.obj is records[0]
    expected_payload = bytes(payload)
    assert bytes(memoryview(records[0])) == expected_payload

    # A derived view owns the Record exporter and keeps the Rust Bytes alive.
    del records
    assert bytes(payload) == expected_payload
    assert loader.metrics()["prefetched_shards"] == dataset.shard_count


def test_reader_populates_and_reuses_local_cache(
    dataset: shardpack.Dataset, tmp_path: Path
) -> None:
    cache = tmp_path / "cache"
    cold = dataset.records(cache_dir=cache)
    cold_ids = [record.sample_id for record in cold]
    assert cold.metrics()["cache_misses"] == dataset.shard_count
    assert cold.metrics()["cache_downloaded_bytes"] == dataset.stored_bytes

    hot = dataset.records(cache_dir=cache)
    assert [record.sample_id for record in hot] == cold_ids
    assert hot.metrics()["cache_hits"] == dataset.shard_count


def test_reader_rejects_invalid_cache_and_prefetch_configuration(
    dataset: shardpack.Dataset, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="cache_mode"):
        dataset.records(cache_mode="unknown")
    with pytest.raises(ValueError, match="requires cache_dir"):
        dataset.records(cache_mode="plaintext")
    with pytest.raises(RuntimeError, match="network_concurrency"):
        dataset.records(network_concurrency=0)
    with pytest.raises(RuntimeError, match="network_prefetch_bytes"):
        dataset.records(
            cache_dir=tmp_path / "cache",
            network_concurrency=4,
            network_prefetch_bytes=3,
        )


def test_simple_reader_partitions_shards_across_distributed_consumers(
    dataset: shardpack.Dataset,
) -> None:
    all_ids = {record.sample_id for record in dataset.records(shard_seed=19)}
    rank_zero = {
        record.sample_id for record in dataset.records(shard_seed=19, rank=0, world_size=2)
    }
    rank_one = {record.sample_id for record in dataset.records(shard_seed=19, rank=1, world_size=2)}

    assert rank_zero.isdisjoint(rank_one)
    assert rank_zero | rank_one == all_ids


def test_framework_neutral_source_partitions_ranks(
    dataset: shardpack.Dataset, tmp_path: Path
) -> None:
    config = tmp_path / "source.toml"
    config.write_text(
        f'''\
version = 1
default_store = "local"

[stores.local]
type = "filesystem"
root = "{DATA}"
'''
    )
    rank_zero = {
        record.sample_id
        for record in shardpack.ShardPackSource(
            dataset.layout_key, config_path=config, rank=0, world_size=2, shard_seed=19
        )
    }
    rank_one = {
        record.sample_id
        for record in shardpack.ShardPackSource(
            dataset.layout_key, config_path=config, rank=1, world_size=2, shard_seed=19
        )
    }

    assert rank_zero.isdisjoint(rank_one)
    assert rank_zero | rank_one == {bytes([value]) * 16 for value in range(7)}


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
    dataset = shardpack.Dataset(layout_key, config_path=config)

    with pytest.raises(ValueError, match="provided together"):
        dataset.records(encryption_key_id="test://python-key")
    with pytest.raises(ValueError, match="exactly 32 bytes"):
        dataset.records(encryption_key_id="test://python-key", encryption_key=b"short")
    key_path = tmp_path_factory.mktemp("keys") / "dataset.key"
    key_path.write_bytes(bytes([0x77]) * 32)
    with pytest.raises(ValueError, match="mutually exclusive"):
        dataset.records(
            encryption_key_id="test://python-key",
            encryption_key=bytes([0x77]) * 32,
            encryption_key_path=key_path,
        )

    records = list(
        dataset.records(
            encryption_key_id="test://python-key",
            encryption_key_path=key_path,
        )
    )
    assert len(records) == 7

    plaintext_cache = tmp_path_factory.mktemp("plaintext-cache")
    cold = dataset.records(
        cache_dir=plaintext_cache,
        cache_mode="plaintext",
        encryption_key_id="test://python-key",
        encryption_key=bytes([0x77]) * 32,
    )
    cold_ids = [record.sample_id for record in cold]
    assert cold.metrics()["cache_misses"] == dataset.shard_count

    hot = dataset.records(
        cache_dir=plaintext_cache,
        cache_mode="plaintext",
        encryption_key_id="test://python-key",
        encryption_key=bytes([0x77]) * 32,
    )
    assert [record.sample_id for record in hot] == cold_ids
    assert hot.metrics()["cache_hits"] == dataset.shard_count
