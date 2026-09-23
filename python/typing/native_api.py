"""Static-only checks: uv run mypy --follow-imports=silent typing/native_api.py."""

from typing import assert_type

from shardpack import Dataset, Record, RecordLoader, _native


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
