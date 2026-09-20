# MVP roadmap

- Done: binary format, corruption checks and sequential None/Zstd round trips.
- Done: protobuf snapshot/layout manifests, validation and deterministic IDs.
- Done: OpenDAL storage abstraction, immutable publication and content-addressed paths.
- Done: encoded source/codec interfaces and bounded multi-shard materialization.
- Done: byte-preserving ShardPack repacking and content validation.
- Done: deterministic distributed assignment is integrated into `DatasetReader`;
  rank/worker topology and shard seed survive checkpoints and resume. Runtime
  encoded-record shuffle is bounded by both records and bytes.
- Done: stable node affinity uses rendezvous hashing before node-local epoch shuffle
  and rank/worker partitioning. Membership reorder causes no movement, scale-out
  only moves shards won by the new node, and checkpoints retain the exact node set.
- Done: streaming verified local cache and basic capacity eviction.
- Done: block-boundary checkpoints and OpenDAL range-based resume.
- Done: standalone import of existing shards with streaming CRC, fingerprint and
  exact-byte identity verification.
- Done: exact-byte BLAKE3 verification on the normal uncached runtime path.
- Done: persistent preferred-layout generations and job/manual/retention layout pins.
- Done: GC roots derived from published lifecycle, preferred layouts and persistent pins.
- Done: object-store inventory and orphan shard discovery from manifests plus catalog roots.
- Done: publication/sweep concurrency barrier with exact-owner crash recovery.
- Done: multi-shard asynchronous copy-on-write rebalance with pre-publication
  equivalence validation and stale-source-safe preferred-layout CAS.
- Done: idempotent retirement records and retirement-time-based GC grace protection.
- Done: bounded exponential retry for transport/truncation/checksum failures with
  block-boundary Range GET recovery and fail-closed exhaustion behavior.
- Done: explicit shuffle/decode/transform/batch pipeline with record- and
  byte-bounded queues, worker limits, backpressure, cancellation and batch prefetch.
- Done: typed codec registry with built-in/embedded schema validation and runtime dispatch.
- Done: deterministic sample/group-aware split policies and split-specific layouts.
- Done: deterministic single-pass offline shuffle bounded by records and encoded bytes.
- Done: v1 binary golden vector, generated round-trip properties, exhaustive
  single-bit corruption/truncation tests, and resource-bounded cargo-fuzz targets
  for plain/encrypted shards plus snapshot/layout protobuf manifests.
- Done: local/S3 operational CLI for inspect, full-stream verify, standalone
  import, copy-on-write rebalance, pin/unpin, and GC plan/apply. Generic ingestion
  remains a library API until the deferred Python SDK defines its transport.
- Done: `network_concurrency` and `network_prefetch_bytes` drive concurrent,
  ordered multi-shard compressed-byte prefetch. Each in-flight shard receives an
  independent byte budget, preventing later shards from starving the current
  stream while preserving deterministic shard order and backpressure.
- Done: exact pipeline safe-point checkpoints stop source admission at a
  deterministic batch boundary, preserve block-level loader position plus every
  shuffle record and RNG state, sequence/reorder parallel worker output, drain all
  admitted decode/transform work, and resume without loss, duplication, or order
  change. Pending batches must be processed before committing the checkpoint.
- Done: runtime `JobPin` acquires an exact persistent `SnapshotId + LayoutId`
  running-job root and explicitly releases it after clean shutdown. A crashed
  process intentionally leaves the pin for recovery or operator cleanup.
- Done: concurrent misses converge on one verified hard-linked entry, corrupt
  deletion tolerates races, stale crash partials are cleaned explicitly, and a
  cross-process filesystem lock serializes publication/eviction/capacity scans.
  Verified hits return an already-open descriptor so eviction cannot invalidate an
  active reader. An individually oversized protected shard may temporarily exceed
  the configured soft capacity, by design.
- Done: `MissingDataPolicy::{Fail, Skip}` persists expected/written/skipped
  counts, publishes a distinct `Incomplete` layout state, requires explicit
  runtime opt-in, survives byte-preserving rebalance, and can never satisfy
  durable last-copy protection. Arbitrary source errors remain fail-closed.
- Done: S3 capability tests prove conditional immutable-create support, and an
  opt-in destructive-safe MinIO/S3 suite covers multi-part-scale streaming,
  duplicate-create rejection, cross-part ranges, range resume, listing, not-found
  mapping and exact-key deletion. The live suite requires a disposable bucket.
- Done: checked-in v1 shard/snapshot/layout golden fixtures are decoded and
  reproduced in CI, with a documented binary/manifest/codec compatibility matrix
  and protobuf evolution rules.
- Done: cloneable lock-free metrics expose runtime records/bytes, verified shards,
  prefetches, retries, checksum failures and queue backpressure, plus cache
  hits/misses, downloaded bytes, corruption, eviction and stale cleanup. Plain
  snapshots are vendor-neutral and can be exported by the host telemetry stack.
- Done: optional encrypted v2 shards protect the directory and compressed payload
  with AWS-LC AES-256-GCM, external key IDs, per-shard derived keys, block-boundary
  recovery, zeroized in-process keys, and a retained cross-provider throughput bench.
- Deferred: Python SDK and Python-side adapters, including optional Lance input.

Every stage must keep published objects immutable and retain end-to-end tests that
train/read without access to any canonical source.
