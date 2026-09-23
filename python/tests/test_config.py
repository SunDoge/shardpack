import json
import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError

from shardpack import Dataset, ShardPackSource
from shardpack.api import _config_args
from shardpack.config import (
    FilesystemConfig,
    InlineCredentials,
    S3Config,
    ShardPackConfig,
    config_json_schema,
)

ROOT = Path(__file__).resolve().parents[2]


def test_generated_schema_and_examples_are_current():
    schema = json.loads((ROOT / "schemas/shardpack-config.schema.json").read_text())
    assert schema == config_json_schema()
    for path in (ROOT / "examples/config").glob("*.toml"):
        config = ShardPackConfig.model_validate(tomllib.loads(path.read_text()))
        assert "training" in config.stores


def test_model_config_reaches_native_reader_and_source():
    data = Path(__file__).parent / "data"
    config = ShardPackConfig(stores={"local": FilesystemConfig(root=data)})
    layout = (data / "layout-key.txt").read_text().strip()
    dataset = Dataset(layout, config=config)
    assert dataset.record_count == 7
    assert len(list(dataset.records())) == 7
    assert len(list(ShardPackSource(layout, config=config))) == 7


@pytest.mark.parametrize(
    "value",
    [
        {"stores": {}},
        {"version": 2, "stores": {"local": {"type": "filesystem", "root": "/tmp"}}},
        {"default_store": "missing", "stores": {"local": {"type": "filesystem", "root": "/tmp"}}},
        {"stores": {"local": {"type": "s3", "bucket": "test", "endpont": "typo"}}},
        {"stores": {"local": {"type": "s3", "bucket": "test", "write_concurrency": 0}}},
        {"stores": {"bad/name": {"type": "filesystem", "root": "/tmp"}}},
        {"stores": {"local": {"type": "s3", "bucket": "test", "credentials": {"type": "env"}}}},
    ],
)
def test_invalid_configs_are_rejected(value):
    with pytest.raises(ValidationError):
        ShardPackConfig.model_validate(value)


def test_inline_secrets_are_not_masked_in_native_transport_or_exposed_in_repr():
    config = ShardPackConfig(
        stores={
            "training": S3Config(
                bucket="test",
                credentials=InlineCredentials(
                    access_key_id="test-id", secret_access_key="test-secret"
                ),
            )
        }
    )
    assert "test-secret" not in repr(config)
    transported = json.loads(_config_args(config, None)["config"])
    assert transported["stores"]["training"]["credentials"]["secret_access_key"] == "test-secret"
    assert transported["stores"]["training"]["write_chunk_bytes"] == 16_777_216
