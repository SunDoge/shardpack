# ShardPack Python SDK

Native Python bindings for the bounded ShardPack training loader.

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
