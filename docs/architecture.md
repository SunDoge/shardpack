# ShardPack architecture

## Status

This document records the MVP architecture. The normative product requirements are
the ShardPack specification; this document explains how the Rust workspace enforces
them.

## Crate boundaries

The workspace is split from day one so the portable format never acquires a runtime
dependency on Lance, S3, or a particular training framework.

| Crate | Responsibility | Must not depend on |
|---|---|---|
| `shardpack-format` | Fixed-width binary container, CRC32C, BLAKE3 identity, content fingerprint, sequential reader/writer | manifests, object stores, schemas |
| `shardpack-manifest` | Authoritative protobuf manifests, typed IDs, validation and canonical ID derivation | storage implementations, Lance |
| `shardpack-codec` | Typed decoder registry and portable schema compatibility validation | container framing, storage |
| `shardpack-storage` | OpenDAL operator wrapper, immutable streaming writes and range reads | record codecs, manifests |
| `shardpack-core` | Record codecs, source readers, materialization, rebalance and publication validation | training framework |
| `shardpack-runtime` | Deterministic shard assignment, bounded encoded-record shuffle, checkpoints | Lance, record schema details |
| `shardpack-catalog` | Append-only preferred-layout generations and persistent layout pins | record payload semantics |
| `shardpack-cli` | Thin operational frontend | private format implementation details |

Rust core accepts an encoded record stream and has no Lance dependency. A future
Python SDK may scan Lance (or any Python data source), resolve raw assets, encode
self-contained records, and feed that stream into the core. Storage uses the same
Apache OpenDAL operator API for local filesystem tests and S3; backend selection
must not change ShardPack semantics.

## Dependency direction

```text
format        manifest        storage
   \             |             /
    +------------+------------+
                 |
                core
                 |
              runtime
                 |
                cli
```

`runtime` may consume core abstractions, but the persistent format does not know
about runtime scheduling. The CLI composes crates; it contains no data semantics.

## Binary format v1

A shard starts with a 64-byte little-endian `ShardHeader`, followed only by blocks.
Each block starts with a 64-byte `BlockHeader`, a sequence of 24-byte directory
entries, then one stored payload. There is no footer and writers never seek.

The block checksum covers directory and stored payload bytes. Header checksums cover all header
bytes preceding checksum fields. `ShardId` is BLAKE3 over every exact byte written,
including headers. A normal record cannot cross a block; an oversized record gets a
single-record jumbo block.

Record payloads are opaque. Only the directory exposes stable 128-bit `SampleId`,
payload size, and flags. Block codecs in v1 are `none` and `zstd`. The zstd
writer uses zrip's reusable transfer-oriented encoder; standard frames are read
with the reference C decoder for stronger decode throughput across data shapes.
Block and shard soft-target accounting includes each record's 24-byte directory
entry as well as its payload. Tiny or empty records therefore cannot create an
unbounded directory or one unexpectedly enormous shard.

## Content equivalence

For each record:

```text
digest = BLAKE3("shardpack-record-v1" || sample_id || payload)
```

Two independent 256-bit modular accumulators are updated from domain-separated
derivations of this digest. Together with the record count they form an
order-independent multiset fingerprint. It deliberately excludes flags: rebalance
must preserve IDs and exact payload bytes, while container metadata may evolve.

Fingerprints are an equivalence guard, not a proof against an adversary. A
rebalance publish requires equal record counts and fingerprints, and every target
shard must already be finalized and verified.

## Publication model

Objects are written under immutable content-derived names. A layout is visible only
after all objects finish, their byte hashes and aggregate fingerprint validate, and
the complete layout manifest is atomically published. Preferred-layout switching is
a separate control-plane operation. Readers pin `SnapshotId + LayoutId` for a job.

`LayoutManifest` remains immutable and records a finalized layout as `Complete`.
Mutable lifecycle such as preferred, published, retired, job-pinned, or
retention-protected belongs to a separate catalog record and is excluded from
`LayoutId`. GC consumes catalog state; it never rewrites a published manifest.

Publication/control-plane mutations and GC sweep share an object-store
coordination barrier acquired with conditional create. A crashed owner is never
evicted from wall-clock guesses: recovery requires the exact recorded owner ID.
This favors data safety over automatic availability during ambiguous failures.

Dataset mirroring copies every shard byte-for-byte, validates exact identities and
the aggregate content fingerprint at the destination, and only then publishes the
original authoritative snapshot/layout protobuf bytes. It is idempotent and does
not require the canonical source or a record decoder.

Readers checkpoint before each potentially blocking read. Retryable transport,
truncation, and checksum failures reopen the immutable object at the containing
block boundary and validate the complete block again. Without a cache this uses
a Range GET. With a cache, resume consumes the cached/tee stream from byte zero
to the boundary so a cold retry leaves behind one complete verified shard rather
than an unusable suffix; BLAKE3 therefore still covers the entire object, including
the discarded prefix. Already-consumed records in the block are then discarded.
An exact-identity mismatch is retried only through that full cached path; a suffix
Range GET cannot prove the prefix and therefore fails closed. Retry exhaustion
fails the shard; records are never silently skipped.

Block-boundary readers also receive an explicit expected encryption mode from
the pinned layout/key context. They reject a block whose mode differs from the
shard, even though resume intentionally does not reread the shard header.

The runtime pre-opens a bounded window of future shards and buffers compressed
object bytes, not decoded records or tensors. Each concurrent shard owns a fixed
share of `network_prefetch_bytes`; this avoids a future shard consuming all permits
needed by the current ordered stream. Dropping the reader closes the channels and
cancels blocked producers through normal backpressure teardown.

Pipeline records receive monotonically increasing sequence numbers before parallel
decode. The batch stage reorders completed work by that sequence, so worker timing
does not change training order. Decode, transform, and batch/output buffering have
separate byte budgets. A transformed value transfers from the transform budget to
`batch_queue_bytes` while it is reordered, accumulated, or waiting in an output
batch; a batch that cannot fit fails explicitly instead of deadlocking or exceeding
the configured bound. A checkpoint request stops new source admission at
a full batch boundary, captures the block-level loader position and the complete
shuffle pool/RNG state, then closes admission and drains every downstream queue.
`quiesce` returns any already-admitted batches together with the checkpoint; callers
must process those batches before durably committing it. Resume starts sequence
numbering at the saved boundary and restores the exact future encoded-record order.

`RuntimeMetrics` is a cloneable handle backed by relaxed atomics. Readers and
pipelines update record/byte throughput, verified shards, prefetches, retries,
checksum failures, and byte-queue backpressure without locks on the hot path. A
trainer can periodically call `snapshot()` and export the resulting plain value to
its own metrics system; the core does not mandate a telemetry vendor.

`DatasetReader::new_assigned` deterministically hashes and shuffles immutable
shard IDs for an epoch, then partitions them across rank and worker topology before
opening any object. The checkpoint stores that topology and shard seed; resume
reconstructs the identical assigned plan before applying block coordinates.

For nodes with persistent local shard caches, `DatasetReader::new_affinity_assigned`
is the preferred entry point. `NodeAffinity` contains a stable current node ID, the
complete node membership, an affinity seed, and the node-local rank/worker topology.
Highest-random-weight (rendezvous) hashing first assigns every shard to a node without
using the epoch seed. Shards therefore remain on the same node across epochs. The
epoch seed only reshuffles that node's assigned stream before local rank/worker
partitioning. Membership order is irrelevant; adding or removing a node remaps only
shards whose rendezvous winner changes. The exact affinity configuration is stored
in loader checkpoints, so resume cannot silently fall back to global rank modulo
assignment.

The stable node ID must describe the cache locality rather than a transient process.
Good values include a Kubernetes node UID or a durable GPU host identity. Pod names,
global ranks, and process IDs are poor values because they change on restart. Every
participant must use the same membership set and `affinity_seed`; the control plane
is responsible for freezing them for the lifetime of a running epoch/job.

On a cache hit, the local layer opens the Tokio file asynchronously and performs
only a metadata length check; it does not pre-read the shard. The normal sequential
reader computes block CRCs and the exact-byte BLAKE3 digest while delivering records,
so integrity remains end-to-end without doubling NVMe traffic. A failed read or
checksum invalidates the content-addressed cache entry before retry, causing the
immutable shard to be fetched and atomically published again. Local regular-file
I/O retains the common `AsyncRead` interface (Tokio may use its blocking file-I/O
pool internally), keeping S3 and cache sources interchangeable above this layer.
Concurrent cold misses are coalesced across processes by a per-shard, asynchronously
polled filesystem lock: one reader streams from object storage while waiters consume
the verified local entry afterward. Locks use 65,536 hash stripes, bounding internal
lock-file growth; different stripes continue downloading concurrently and cache
inventory scans skip the lock directory.

## Portability and durability

`SelfContained` is the default and promises that the snapshot manifest plus its
referenced shards suffice for decoding and training. Source fields are provenance
only. Standalone imports default to `Durable`; GC must retain at least one complete,
durable, self-contained layout. Source availability never overrides this policy.

Optional encryption is a layout property. The container encrypts each complete
block body after compression with AES-256-GCM, while manifests contain only an
algorithm and external `key_id`. Credentials are injected into readers and writers;
they are never serialized. Re-encryption and key rotation are byte-preserving at
the logical record level and therefore keep the snapshot and content fingerprint.
See [encryption.md](encryption.md) for wire, recovery, and benchmark details.

Missing input is fail-closed by default. An adapter may explicitly use
`MissingDataPolicy::Skip { skipped_records }`; publication then records
expected/written/skipped counts and uses `LayoutState::Incomplete`. Training
readers reject such layouts unless the caller opts in, CLI inspection exposes the
counts, and GC never treats an incomplete layout as the last durable copy.
