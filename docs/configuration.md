# Configuration

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
