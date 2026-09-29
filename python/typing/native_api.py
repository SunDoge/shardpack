"""Static-only checks: uv run mypy --follow-imports=silent typing/native_api.py."""

from typing import assert_type

from shardpack import Dataset, Record, RecordLoader, SamplePool, SamplePoolSource, _native


def check(native: _native.Dataset, public: Dataset, writer: _native.DatasetWriter) -> None:
    assert_type(native.layout_id, bytes)
    assert_type(native.record_count, int)
    assert_type(native.records(), _native.RecordLoader)
    assert_type(public.records(), RecordLoader)
    record = next(native.records())
    assert_type(record, Record)
    assert_type(record.sample_id, bytes)
    assert_type(record.flags, int)
    assert_type(record.payload, memoryview)
    assert_type(native.records().metrics(), dict[str, int])
    assert_type(writer.finish(), str)
    assert_type(_native.EpochScheduler().next(), tuple[int, int, int])
    assert_type(_native.cli_main(["shardpack", "--help"]), int)
    pool = SamplePool(max_records=16, max_bytes=1024)
    pool.insert(record)
    assert_type(pool.sample(1), list[Record])
    assert_type(pool.available(exclude_ids=[record.sample_id]), int)
    assert_type(pool.buffered_bytes, int)
    group = next(iter(SamplePoolSource([record])))
    assert_type(group.primary, Record)
    assert_type(group.extras, tuple[Record, ...])
