# ShardPack CLI

For persistent or multiple storage backends, use the typed TOML
[configuration](configuration.md):

```bash
shardpack --config ~/.config/shardpack/config.toml --store training \
  inspect layout layouts/example.layout.pb
```

The CLI uses the same OpenDAL-backed object API for local filesystems and S3.
Object names passed to commands are normalized keys relative to the selected
backend root, never host filesystem paths.

```bash
# Local development and tests
shardpack --root ./dataset verify layouts/<layout-id>.layout.pb

# AWS S3; credentials use the normal AWS provider chain
shardpack --s3-bucket training-data --s3-root shardpack \
  verify layouts/<layout-id>.layout.pb

# MinIO or another S3-compatible service
AWS_ACCESS_KEY_ID=minio AWS_SECRET_ACCESS_KEY=minio-secret \
shardpack --s3-bucket training --s3-endpoint http://127.0.0.1:9000 \
  verify layouts/<layout-id>.layout.pb
```

S3 multipart uploads default to eight concurrent 16 MiB parts, bounding one
object writer to roughly 128 MiB of buffered data. Tune them with
`--s3-write-concurrency` / `SHARDPACK_S3_WRITE_CONCURRENCY` and
`--s3-write-chunk-bytes` / `SHARDPACK_S3_WRITE_CHUNK_BYTES`.

`inspect` validates a snapshot or layout manifest before printing its summary.
`verify` sequentially reads every referenced shard and checks headers, block CRCs,
record counts, content fingerprints, byte sizes, and exact BLAKE3 shard IDs.

`import` creates a durable, self-contained snapshot and layout around existing
`.spk` objects. It reads every shard before publication. Record schema IDs are
64 hexadecimal characters; an optional embedded schema file is copied into the
snapshot manifest.

```bash
shardpack --root ./dataset import \
  --dataset-id images-v1 \
  --codec-name example.protobuf \
  --codec-version 1 \
  --schema-id <64-hex-characters> \
  incoming/part-000.spk incoming/part-001.spk
```

`rebalance` is copy-on-write. It validates record count and the order-independent
content fingerprint, publishes the complete replacement, advances preferred state
with compare-and-switch, and only then records retirement of the source layout.

GC is deliberately two-step. `gc-plan` never mutates storage. `gc-apply` requires
`--yes`, repeats inventory under the shared publication barrier, enforces durable
last-copy protection, and deletes only the resulting plan.

```bash
shardpack --root ./dataset gc-plan
shardpack --root ./dataset gc-apply --yes
```

Secrets should normally come from `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`. Supplying credentials as CLI
arguments is supported for automation but can expose them through process listings.

The repeatable local integration test uses the RustFS compose project at
`/data/rustfs`. It starts RustFS with the shared-disk safety check bypassed only
for local testing, waits for health, creates a disposable test bucket through
an environment-only rclone configuration, and runs the ignored S3 suite:

```bash
./scripts/test-rustfs.sh
```

`RUSTFS_DIR`, `SHARDPACK_TEST_S3_BUCKET`, `SHARDPACK_TEST_S3_ENDPOINT`,
`AWS_REGION`, and the standard AWS credential variables can override its
defaults. The RustFS container remains running so repeated test runs are fast.

The same opt-in test can also target MinIO or a real S3 bucket directly:

```bash
SHARDPACK_TEST_S3_BUCKET=training-test \
SHARDPACK_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
AWS_ACCESS_KEY_ID=minio AWS_SECRET_ACCESS_KEY=minio-secret \
cargo test -p shardpack-storage --test s3 -- --ignored --nocapture
```

It creates one process- and timestamp-scoped object and removes that exact object
after checking streaming, range reads, listing, and conditional create semantics.
