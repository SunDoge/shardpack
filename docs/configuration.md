# Configuration

For one S3 store using the default credential chain, this is enough:

```toml
[stores.training]
type = "s3"
bucket = "my-dataset-bucket"
region = "us-west-2"
```

For S3-compatible providers add `endpoint`. `version`, `default_store`, catalog
settings and upload tuning can be omitted for a single-store configuration.

## Typed Python configuration and editor schema

```python
from shardpack import Dataset
from shardpack.config import ShardPackConfig, S3Config

config = ShardPackConfig(stores={
    "training": S3Config(bucket="my-dataset-bucket", region="us-west-2"),
})
dataset = Dataset("layouts/example.layout.pb", config=config)
```

`Dataset`, `DatasetWriter`, and `ShardPackSource` accept this model as well as
ordinary dictionaries and `config_path`. Pydantic validates model construction;
Rust still applies environment overrides and runtime validation. TOML files used
via `config_path` are loaded by Rust, so native callers need no Python dependency.

The [JSON Schema](../schemas/shardpack-config.schema.json) is generated from
`shardpack.config` using Pydantic's
[model_json_schema](https://docs.pydantic.dev/latest/concepts/json_schema/).
The example TOML files have a `#:schema` annotation for schema-aware editors such
as Taplo. If you copy an example elsewhere, update this path to the schema's
absolute location or configure your editor's schema association. JSON Schema
checks structure; references such as `default_store` pointing to an existing store
are validated by Pydantic/Rust, not by the structural schema.

Regenerate or check it with:

```bash
cd python
uv run python ../scripts/generate-config-schema.py
uv run python ../scripts/generate-config-schema.py --check
```

Inline credentials are omitted from model repr, but explicit model serialization
contains their values so they can reach the native client. Do not log serialized
configs. Prefer the default credential chain or environment-variable references.

## Full configuration

ShardPack accepts typed TOML through `--config`, `SHARDPACK_CONFIG`, or
`$XDG_CONFIG_HOME/shardpack/config.toml`. Explicit CLI storage flags remain
available for one-off, single-store operation.

Copyable example files:

- [S3 upload/read configuration](../examples/config/s3.toml)
- [Public HTTP/CDN training configuration](../examples/config/http.toml)

For the S3 example, set `SHARDPACK_S3_ACCESS_KEY_ID` and
`SHARDPACK_S3_SECRET_ACCESS_KEY` in the environment. The `credentials` fields name
these variables; they do not contain their values. Pass the file in Python with
`Dataset(layout_key, config_path="/path/to/s3.toml")` or
`DatasetWriter(snapshot_id, staging, config_path="/path/to/s3.toml")`.

```toml
version = 1
default_store = "training"

[catalog]
store = "training"
prefix = "catalog"

[stores.training]
type = "s3"
bucket = "training-datasets"
region = "us-west-2"
root = "shardpack"
write_chunk_bytes = 16777216
write_concurrency = 8

[stores.training.credentials]
type = "profile"
profile = "production"

[stores.archive]
type = "s3"
endpoint = "https://account.example-object-storage.com"
bucket = "dataset-archive"
region = "auto"

[stores.archive.credentials]
type = "env"
access_key_id = "ARCHIVE_ACCESS_KEY_ID"
secret_access_key = "ARCHIVE_SECRET_ACCESS_KEY"

[stores.local]
type = "filesystem"
root = "/data/shardpack"

[stores.public_training]
type = "http"
base_url = "https://data.example.com/shardpack"
```

Credential modes are `default_chain`, `profile`, `env`, and `inline`. Prefer the
provider default chain, a named profile, or names of environment variables.
Inline credentials are accepted for S3-compatible test deployments and are
redacted from Rust `Debug` output, but the TOML file itself still contains the
secret and must be protected accordingly.

Use `--store NAME` to override `default_store`. The catalog store is independent
from the selected data store. Configuration is operational state and is never
embedded in snapshot or layout manifests.

## Multiple object stores

Every shard location in a layout contains a `store_id` and normalized
`object_key`. The runtime resolves each shard through the configured store
registry, so one immutable layout may reference multiple S3-compatible providers
and local storage at the same time.

Import arguments normally refer to the selected store. Prefix an object key with
`STORE_ID::` to select another configured store:

```bash
shardpack --config ~/.config/shardpack/config.toml import \
  --dataset-id example \
  --codec-name opaque \
  --codec-version 1 \
  --schema-id 0000000000000000000000000000000000000000000000000000000000000000 \
  training::shards/a.spk \
  archive::shards/b.spk
```

Store IDs contain only ASCII letters, digits, `-`, or `_`. Credentials are not
part of ShardPack portability: as with an encryption key, authorization must be
supplied by the runtime. Complete record payloads remain inside the referenced
shard objects.

## Public encrypted delivery

An `http` store is a credential-free, read-only transport for immutable objects
served by a public B2 bucket, Cloudflare CDN, another untrusted mirror, or an
IPFS HTTP gateway. It cannot list, upload, or delete objects. Manifest
`object_key` values are appended to `base_url`.

Use public delivery only with ShardPack-encrypted layouts and supply key material
separately to the training process. The full-shard BLAKE3 identity plus per-block
AEAD authentication make the transport untrusted without changing correctness.
