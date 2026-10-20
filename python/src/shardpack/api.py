"""Ergonomic Python facades over the native ShardPack classes."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from os import PathLike
from typing import Any

from ._native import Dataset as _NativeDataset
from ._native import DatasetWriter as _NativeDatasetWriter
from ._native import RecordLoader
from .config import ShardPackConfig

Config = Mapping[str, Any] | ShardPackConfig


def _json_default(value: object) -> str:
    try:
        return os.fspath(value)  # type: ignore[arg-type]
    except TypeError as error:
        raise TypeError(f"configuration value is not JSON serializable: {value!r}") from error


def _config_args(
    config: Config | None,
    config_path: str | PathLike[str] | None,
) -> dict[str, str]:
    if (config is None) == (config_path is None):
        raise ValueError("exactly one of config or config_path is required")
    if config is not None:
        if isinstance(config, ShardPackConfig):
            return {"config": config.model_dump_json(exclude_none=True)}
        return {"config": json.dumps(config, default=_json_default)}
    return {"config_path": os.fspath(config_path)}


class Dataset:
    """An immutable layout opened from an inline mapping or TOML path."""

    def __init__(
        self,
        layout: str,
        *,
        config: Config | None = None,
        config_path: str | PathLike[str] | None = None,
        store: str | None = None,
        expected_snapshot_id: bytes | None = None,
        expected_layout_id: bytes | None = None,
    ) -> None:
        self._native = _NativeDataset(
            layout,
            **_config_args(config, config_path),
            store=store,
            expected_snapshot_id=expected_snapshot_id,
            expected_layout_id=expected_layout_id,
        )

    @property
    def layout_id(self) -> bytes:
        return self._native.layout_id

    @property
    def snapshot_id(self) -> bytes:
        return self._native.snapshot_id

    @property
    def record_count(self) -> int:
        return self._native.record_count

    @property
    def shard_count(self) -> int:
        return self._native.shard_count

    @property
    def stored_bytes(self) -> int:
        return self._native.stored_bytes

    @property
    def layout_key(self) -> str:
        return self._native.layout_key

    def records(self, **options: Any) -> RecordLoader:
        return self._native.records(**options)

    def validate_epoch(
        self, *, shard_seed: int, world_size: int, batch_size: int, drop_last: bool = False
    ) -> list[int]:
        return self._native.validate_epoch(
            shard_seed=shard_seed,
            world_size=world_size,
            batch_size=batch_size,
            drop_last=drop_last,
        )


class DatasetWriter:
    """Streaming writer configured from an inline mapping or TOML path."""

    def __init__(
        self,
        snapshot_id: bytes,
        staging_dir: str | PathLike[str],
        *,
        config: Config | None = None,
        config_path: str | PathLike[str] | None = None,
        **options: Any,
    ) -> None:
        self._native = _NativeDatasetWriter(
            snapshot_id,
            staging_dir,
            **_config_args(config, config_path),
            **options,
        )

    def write_batch(
        self,
        payload_data: object,
        offsets: object,
        sample_ids: object,
        flags: object | None = None,
    ) -> int:
        return self._native.write_batch(payload_data, offsets, sample_ids, flags)

    def finish(self) -> str:
        return self._native.finish()
