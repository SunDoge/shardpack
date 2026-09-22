# ShardPack

[![Rust CI](https://github.com/SunDoge/shardpack/actions/workflows/rust-ci.yml/badge.svg)](https://github.com/SunDoge/shardpack/actions/workflows/rust-ci.yml)

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
- an abi3 Python upload SDK, a framework-neutral bounded threaded loader, and a
  PyTorch adapter with zero-copy payload memoryviews, verified NVMe cache and
  bounded shard prefetch;
- a repeatable `/data/rustfs` S3-compatible integration test harness.

The Rust core intentionally has no Lance dependency. Python/Lance is the offline
filtering, encoding and upload adapter. The Rust runtime owns streaming, cache,
shuffle and epoch state; Python can add framework-neutral threaded native decode
or use the PyTorch compatibility adapter.

See [architecture](docs/architecture.md), [binary format](docs/binary-format.md),
[encryption](docs/encryption.md), [compatibility policy](docs/compatibility.md),
[configuration](docs/configuration.md), [performance](docs/performance.md),
[Cloudflare/B2 deployment](docs/cloudflare-b2.md),
[CLI guide](docs/cli.md), [Python SDK](docs/python.md),
[Python release](docs/python-release.md),
[Python writer design](docs/python-writer.md), and the [MVP roadmap](docs/roadmap.md).

```bash
cargo test --workspace
cargo run -p shardpack-cli -- --help
mise run test:glibc
```
