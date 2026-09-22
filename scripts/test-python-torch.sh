#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$PROJECT_ROOT/python"
uv sync --locked --group torch
uv run maturin develop --release
uv run pytest -q tests/test_pytorch_integration.py

if [[ "${SHARDPACK_RUN_FORY_BENCH:-0}" == "1" ]]; then
    uv run python benchmarks/fory_dataloader.py \
        --records "${SHARDPACK_BENCH_RECORDS:-100000}" \
        --workers "${SHARDPACK_BENCH_WORKERS:-4}"
fi
