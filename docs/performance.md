# Performance

ShardPack keeps executable microbenchmarks in the repository. Results below
were measured on the development x86-64 host and are directional rather than a
cross-machine guarantee.

## Format hot path

Run:

```bash
cargo bench -p shardpack-format --bench format_throughput
```

For 128 MiB of 256 KiB records packed into 16 MiB blocks, removing whole-block
temporary copies raised `none` write throughput from about 0.38 to 0.81 GiB/s
and full-validation read throughput from about 1.45 to 2.17 GiB/s. Training
reads skip the redundant order-independent content fingerprint while retaining
CRC32C and exact shard BLAKE3 validation, reaching roughly 3.5--4.5 GiB/s in
repeated runs.

The optimized path:

- computes directory and payload CRC32C separately with `crc32c-fast`, then
  combines the results without concatenating the buffers;
- transfers `None` payload buffers directly into `Bytes` without another copy;
- reuses writer directory and payload capacity between blocks;
- retains content fingerprint generation for materialization, import, and
  rebalance validation, but not for every training epoch.

## Zstd implementation

Run:

```bash
cargo bench -p shardpack-format --bench compression_compare
```

The writer uses `zrip` with a reusable compression context and emits standard
zstd frames. The reader currently uses the reference C zstd decoder. A zrip
`DecompressContext` returning a borrowed output reaches roughly 2.4--3.4 GiB/s
on the retained samples, but ShardPack records must own a block after the next
decode begins. Converting that borrowed output with `into_owned()` adds a full
block copy and lowers measured throughput to roughly 1.1--1.4 GiB/s, below the
C decoder's roughly 1.5--2.0 GiB/s owned-output path. This is an API ownership
bottleneck rather than evidence of an incorrect decoder or slow decode core.

A useful upstream zrip API would combine reusable decoder tables/workspace with
a caller-owned output `Vec`, analogous to `decompress_into`, so consumers can
move the completed allocation into `Bytes` without copying. Until that exists,
the hybrid path avoids the extra block copy.

Local measurements also showed that zrip encoding can outperform C zstd on
incompressible input and produce smaller frames for the structured-data sample
at level 1. The default level is 1 because ShardPack optimizes transfer
throughput rather than archival compression ratio.

The zstd frame checksum is disabled because every stored block already has a
CRC32C, encrypted blocks additionally have an AEAD tag, and full shard reads
validate the exact BLAKE3 identity.

## RustFS / S3-compatible storage

The retained loopback benchmark transfers a 256 MiB object:

```bash
SHARDPACK_TEST_S3_BUCKET=shardpack-integration \
SHARDPACK_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=rustfsadmin \
AWS_SECRET_ACCESS_KEY=rustfsadmin \
cargo test -p shardpack-storage --test s3_throughput --release \
  -- --ignored --nocapture
```

Against `/data/rustfs`, upload throughput rose from about 111 MiB/s with the
serial OpenDAL writer to about 678 MiB/s with eight concurrent 16 MiB multipart
parts. Sixteen parts reached about 701 MiB/s but doubled bounded buffering for a
small gain, so the default remains eight parts (roughly 128 MiB per active
object upload).

Use the RustFS benchmark for regression tracking, not as a prediction of WAN S3
performance. Provider limits, RTT, NIC bandwidth, TLS, and cross-region traffic
can dominate production results.
