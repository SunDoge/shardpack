#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TORCH_TEST_ENV=$(mktemp -d /tmp/shardpack-torch.XXXXXX)
trap 'rm -rf -- "$TORCH_TEST_ENV"' EXIT

wheels=("$PROJECT_ROOT"/python/dist/shardpack-*-cp311-abi3-manylinux_2_17_x86_64.*.whl)
if [[ ! -f "${wheels[0]}" ]]; then
    echo "missing cp311 manylinux 2.17 wheel; run mise run build:python first" >&2
    exit 1
fi

uv venv --python 3.12 "$TORCH_TEST_ENV"
uv pip install \
    --python "$TORCH_TEST_ENV/bin/python" \
    --index-url https://download.pytorch.org/whl/cpu \
    torch
uv pip install \
    --python "$TORCH_TEST_ENV/bin/python" \
    pytest \
    "${wheels[0]}"

cd "$PROJECT_ROOT"
"$TORCH_TEST_ENV/bin/python" -m pytest -q python/tests/test_pytorch_integration.py
