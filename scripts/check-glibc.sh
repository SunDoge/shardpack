#!/usr/bin/env bash
set -euo pipefail

readonly glibc_floor="2.17"
readonly rust_target="x86_64-unknown-linux-gnu"
readonly zig_target="${rust_target}.${glibc_floor}"
readonly cargo_target_dir="${CARGO_TARGET_DIR:-target/zigbuild}"
readonly artifact="${cargo_target_dir}/${rust_target}/release/shardpack"

if ! command -v readelf >/dev/null 2>&1; then
    echo "error: readelf is required to verify the generated ELF symbol versions" >&2
    exit 1
fi

# Resolve Cargo through rustup when available. This avoids machine-global Cargo
# wrappers from replacing cargo-zigbuild's target-aware C compiler wrappers.
if command -v rustup >/dev/null 2>&1; then
    cargo_bin="$(rustup which cargo)"
else
    cargo_bin="$(command -v cargo)"
fi

CARGO_TARGET_DIR="${cargo_target_dir}" "${cargo_bin}" zigbuild \
    --locked \
    --package shardpack-cli \
    --bin shardpack \
    --release \
    --target "${zig_target}"

mapfile -t required_glibc_versions < <(
    readelf --version-info "${artifact}" \
        | grep -oE 'GLIBC_[0-9]+(\.[0-9]+)+' \
        | sed 's/^GLIBC_//' \
        | sort -Vu
)

if ((${#required_glibc_versions[@]} == 0)); then
    echo "error: ${artifact} has no readable GLIBC symbol requirements" >&2
    exit 1
fi

highest_required="${required_glibc_versions[-1]}"
if [[ "$(printf '%s\n%s\n' "${glibc_floor}" "${highest_required}" | sort -V | tail -n 1)" != "${glibc_floor}" ]]; then
    echo "error: ${artifact} requires GLIBC_${highest_required}, above GLIBC_${glibc_floor}" >&2
    exit 1
fi

"${artifact}" --help >/dev/null
echo "verified ${artifact}: maximum required glibc is ${highest_required}"
