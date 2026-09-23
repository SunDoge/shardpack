"""Ergonomic Python facades over the native ShardPack classes."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from os import PathLike
from typing import Any

from ._native import Dataset as _NativeDataset
from ._native import DatasetWriter as _NativeDatasetWriter

Config = Mapping[str, Any]


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
    ) -> None:
        self._native = _NativeDataset(
            layout,
            **_config_args(config, config_path),
            store=store,
        )

    @property
    def layout_id(self) -> bytes:
        return self._native.layout_id

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

    def records(self, **options: Any):
        return self._native.records(**options)


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
