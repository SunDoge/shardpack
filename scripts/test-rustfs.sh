#!/usr/bin/env bash
set -euo pipefail

rustfs_dir="${RUSTFS_DIR:-/data/rustfs}"
endpoint="${SHARDPACK_TEST_S3_ENDPOINT:-http://127.0.0.1:9000}"
bucket="${SHARDPACK_TEST_S3_BUCKET:-shardpack-integration}"
access_key="${AWS_ACCESS_KEY_ID:-rustfsadmin}"
secret_key="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}"
region="${AWS_REGION:-us-east-1}"
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

for command_name in cargo curl docker rclone; do
    command -v "${command_name}" >/dev/null || {
        echo "required command is missing: ${command_name}" >&2
        exit 1
    }
done

test -f "${rustfs_dir}/docker-compose.yml" || {
    echo "RustFS compose file not found: ${rustfs_dir}/docker-compose.yml" >&2
    exit 1
}

(
    cd "${rustfs_dir}"
    RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true docker compose up -d
)

for attempt in $(seq 1 60); do
    if curl --fail --silent --show-error "${endpoint}/health" >/dev/null; then
        break
    fi
    if [[ "${attempt}" == 60 ]]; then
        echo "RustFS did not become healthy at ${endpoint}" >&2
        (cd "${rustfs_dir}" && docker compose logs --no-color --tail=100 rustfs) >&2
        exit 1
    fi
    sleep 1
done

# Use environment-only rclone configuration so credentials are not persisted.
export RCLONE_CONFIG_RUSTFS_TEST_TYPE=s3
export RCLONE_CONFIG_RUSTFS_TEST_PROVIDER=Other
export RCLONE_CONFIG_RUSTFS_TEST_ACCESS_KEY_ID="${access_key}"
export RCLONE_CONFIG_RUSTFS_TEST_SECRET_ACCESS_KEY="${secret_key}"
export RCLONE_CONFIG_RUSTFS_TEST_ENDPOINT="${endpoint}"
rclone mkdir "rustfs_test:${bucket}"

cd "${repo_root}"
SHARDPACK_TEST_S3_BUCKET="${bucket}" \
SHARDPACK_TEST_S3_ENDPOINT="${endpoint}" \
AWS_REGION="${region}" \
AWS_ACCESS_KEY_ID="${access_key}" \
AWS_SECRET_ACCESS_KEY="${secret_key}" \
cargo test -p shardpack-storage --test s3 -- --ignored --nocapture

SHARDPACK_TEST_S3_BUCKET="${bucket}" \
SHARDPACK_TEST_S3_ENDPOINT="${endpoint}" \
AWS_REGION="${region}" \
AWS_ACCESS_KEY_ID="${access_key}" \
AWS_SECRET_ACCESS_KEY="${secret_key}" \
cargo test -p shardpack-cli --test standalone_lifecycle -- --ignored --nocapture
