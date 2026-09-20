# ShardPack

ShardPack is an S3-first, streaming-first, immutable sharded record format and
dataset layout for large-scale machine-learning training.

This repository is an early Rust MVP. The implemented vertical slice includes:

- fixed-width, little-endian shard and block headers;
- stable 128-bit sample IDs in record directories;
- block-level None/Zstd compression and CRC32C validation;
- optional block-level AES-256-GCM encryption with external key IDs;
- exact-byte BLAKE3 shard identities;
- order-independent record-content fingerprints;
- protobuf snapshot/layout manifests with deterministic IDs and validation;
- one Apache OpenDAL API for filesystem tests and S3 production storage;
- byte-preserving ShardPack-to-ShardPack rebalance validation;
- opaque-manifest, byte-preserving snapshot/layout mirroring with source-loss tests;
- bounded multi-shard materialization with content-addressed upload and verified layout publication;
- deterministic node-affine distributed shard assignment and byte-aware bounded shuffle;
- an operational local/S3 CLI for verification, import, rebalance, pinning and GC;
- a repeatable `/data/rustfs` S3-compatible integration test harness.

The Rust core intentionally has no Lance dependency. A future Python SDK will
adapt Python/Lance inputs into encoded record streams consumed by this core.

See [architecture](docs/architecture.md), [binary format](docs/binary-format.md),
[encryption](docs/encryption.md), [compatibility policy](docs/compatibility.md),
[performance](docs/performance.md), [CLI guide](docs/cli.md), and the
[MVP roadmap](docs/roadmap.md).

```bash
cargo test --workspace
cargo run -p shardpack-cli -- --help
```
