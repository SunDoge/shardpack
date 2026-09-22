# ShardPack Python SDK

Native Python bindings for writing and sequentially streaming immutable
ShardPack training datasets from local filesystems and S3-compatible storage.

```bash
pip install shardpack
python -m shardpack --help
```

The package provides packed-buffer dataset writing, verified sequential reads,
distributed shard assignment, bounded record shuffle, NVMe cache population,
and a framework-neutral threaded decode loader. PyTorch is not a runtime
dependency; an optional thin adapter is available when Torch is installed.

Both an inline configuration mapping and a TOML path are supported:

```python
from shardpack import Dataset

dataset = Dataset(
    "layouts/<layout-id>.layout.pb",
    config={
        "version": 1,
        "default_store": "local",
        "stores": {"local": {"type": "filesystem", "root": "/datasets"}},
    },
)
for record in dataset.records(cache_dir="/local-nvme/shardpack"):
    consume(record.sample_id, record.payload)
```

The installed `shardpack` command and `python -m shardpack` execute the same
native Rust CLI without shipping a duplicate binary.

Development uses `uv` and `maturin`:

```bash
uv sync
uv run maturin develop
uv run pytest
```

From the repository root, the equivalent retained checks are:

```bash
mise run test:python
mise run build:python
```

The release task produces a CPython 3.11+ abi3 wheel with a manylinux 2.17
baseline. `uv run maturin sdist --out dist` produces a self-contained source
distribution containing the required Rust workspace crates.
