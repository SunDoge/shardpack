from pathlib import Path

import pytest

from shardpack import Dataset, SamplePool, SamplePoolSource, ShardPackSource, ThreadedLoader

DATA = Path(__file__).parent / "data"


def test_wrapper_forwards_epoch_validation():
    source = ShardPackSource(
        (DATA / "layout-key.txt").read_text().strip(),
        config={"stores": {"local": {"type": "filesystem", "root": DATA}}},
        rank=0,
        world_size=100,
    )
    loader = ThreadedLoader(
        SamplePoolSource(source),
        decode=lambda group: group,
        batch_size=2,
    )
    with pytest.raises(RuntimeError, match="unequal or zero steps"):
        list(loader)


def test_wrapper_closes_upstream_and_forwards_cancel(records):
    from threading import Event

    calls = []

    class Source:
        def iter_with_stop(self, stop, *, batch_size, drop_last):
            calls.append((stop, batch_size, drop_last))
            try:
                yield from records
            finally:
                calls.append("closed")

        def cancel(self):
            calls.append("cancel")

    stop = Event()
    wrapped = SamplePoolSource(Source(), max_records=4, max_bytes=100)
    iterator = wrapped.iter_with_stop(stop, batch_size=3, drop_last=True)
    next(iterator)
    wrapped.cancel()
    iterator.close()
    assert calls == [(stop, 3, True), "cancel", "closed"]
    stop.set()
    assert list(wrapped.iter_with_stop(stop)) == []
    assert len(calls) == 3


@pytest.fixture
def records():
    dataset = Dataset(
        (DATA / "layout-key.txt").read_text().strip(),
        config={
            "default_store": "local",
            "stores": {"local": {"type": "filesystem", "root": DATA}},
        },
    )
    return sorted(dataset.records(), key=lambda r: r.sample_id)


def test_pool_bounds_exclusions_replacement_and_memoryview_lifetime(records):
    pool = SamplePool(max_records=4, max_bytes=100, seed=7)
    for record in records[:4]:
        pool.insert(record)
    assert len(pool) == 4
    assert pool.buffered_bytes == sum(len(r) for r in records[:4])
    selected = pool.sample(3, exclude_ids=[records[0].sample_id])
    assert {r.sample_id for r in selected} == {r.sample_id for r in records[1:4]}
    assert len(pool) == 4
    view = selected[0].payload[1:]
    expected = bytes(view)
    for record in records[4:]:
        pool.insert(record)
    pool.reset(seed=7)
    del selected, pool
    assert view.readonly
    assert bytes(view) == expected

    pool = SamplePool(max_records=4, max_bytes=100)
    pool.insert(records[0])
    pool.insert(records[0])
    assert len(pool) == 1
    assert pool.available(exclude_ids=[records[0].sample_id]) == 0
    assert [r.sample_id for r in pool.sample(3, replace=True)] == [records[0].sample_id] * 3
    with pytest.raises(ValueError, match="eligible"):
        pool.sample(1, exclude_ids=[records[0].sample_id])
    with pytest.raises(ValueError, match="16 bytes"):
        pool.sample(1, exclude_ids=[b"bad"])
    with pytest.raises(ValueError):
        SamplePool(max_records=0, max_bytes=100)
    tiny = SamplePool(max_records=4, max_bytes=1)
    with pytest.raises(ValueError, match="byte limit"):
        tiny.insert(records[0])
    assert len(tiny) == 0


@pytest.mark.parametrize("warmup", ["skip", "replace"])
def test_grouping_preserves_coverage_and_is_worker_independent(records, warmup):
    source = SamplePoolSource(records, max_records=4, max_bytes=100, seed=17, warmup=warmup)

    def decode(group):
        return (group.primary.sample_id, tuple(r.sample_id for r in group.extras))

    expected = [decode(group) for group in source]
    assert [primary for primary, _ in expected] == [r.sample_id for r in records]
    assert expected[0][1] == ()
    if warmup == "skip":
        assert [len(extras) for _, extras in expected[:4]] == [0, 0, 0, 3]
    else:
        assert expected[1][1] == (records[0].sample_id,) * 3
    for primary, extras in expected:
        assert primary not in extras
    for workers in [1, 4]:
        loader = ThreadedLoader(source, decode=decode, decode_workers=workers, batch_size=2)
        assert [item for batch in loader for item in batch] == expected


def test_repeated_primary_ids_are_excluded(records):
    source = SamplePoolSource([records[0], records[1], records[0]], extra_samples=1)
    groups = list(source)
    assert groups[-1].extras[0].sample_id == records[1].sample_id


def test_native_metrics_forward_through_group_source():
    primary = ShardPackSource(
        (DATA / "layout-key.txt").read_text().strip(),
        config={
            "default_store": "local",
            "stores": {"local": {"type": "filesystem", "root": DATA}},
        },
    )
    source = SamplePoolSource(primary)
    loader = ThreadedLoader(source, enable_stats=True)
    assert len(list(loader)) == 7
    assert loader.stats()["source"]["records_read"] == 7


def test_packed_images_to_threaded_four_image_mosaic(tmp_path):
    cv2 = pytest.importorskip("cv2")
    np = pytest.importorskip("numpy")
    from shardpack import DatasetWriter

    config = {
        "default_store": "local",
        "stores": {
            "local": {
                "type": "filesystem",
                "root": tmp_path / "objects",
            }
        },
    }
    payloads = []
    for index in range(8):
        ok, encoded = cv2.imencode(".png", np.full((8, 8, 3), index * 20, dtype=np.uint8))
        assert ok
        payloads.append(encoded.tobytes())
    writer = DatasetWriter(
        bytes([7]) * 32, tmp_path / "staging", config=config, target_block_bytes=256
    )
    offsets = np.array([0, *np.cumsum([len(p) for p in payloads])], dtype=np.uint64)
    ids = np.repeat(np.arange(8, dtype=np.uint8)[:, None], 16, axis=1)
    writer.write_batch(b"".join(payloads), offsets, ids)
    source = SamplePoolSource(
        ShardPackSource(writer.finish(), config=config), max_records=8, max_bytes=8192
    )

    def mosaic(group):
        def image(record):
            return cv2.imdecode(np.frombuffer(record.payload, dtype=np.uint8), cv2.IMREAD_COLOR)

        primary = image(group.primary)
        if not group.extras:
            result = cv2.resize(primary, (16, 16))
        else:
            images = [primary, *(image(r) for r in group.extras)]
            result = np.concatenate(
                (np.concatenate(images[:2], axis=1), np.concatenate(images[2:], axis=1)), axis=0
            )
            assert len({r.sample_id for r in group.extras}) == 3
            assert group.primary.sample_id not in {r.sample_id for r in group.extras}
        return group.primary.sample_id[0], len(group.extras), result

    results = [
        item
        for batch in ThreadedLoader(source, decode=mosaic, decode_workers=4, batch_size=2)
        for item in batch
    ]
    assert [index for index, _, _ in results] == list(range(8))
    assert [extras for _, extras, _ in results] == [0, 0, 0, 3, 3, 3, 3, 3]
    for index, _, image in results:
        assert image.shape == (16, 16, 3)
        assert np.all(image[:8, :8] == index * 20)
