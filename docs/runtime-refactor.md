# Runtime refactor: first stage

This pre-release refactor establishes trust/lifecycle boundaries and rejects
unsafe distributed defaults. It does not change `.spk`, payload bytes, encryption,
compression or shuffle selection. It does **not** complete distributed balancing
or decoded-byte budgets, and makes no new throughput claim.

## Identity, not location

Rust `DatasetIdentity { snapshot_id, layout_id }` validates the manifest against
trusted IDs. `load_trusted_layout(store, key, identity)` is the untrusted-transport
entry point; plain `load_layout` remains an inspection/trusted-transport API.

Python `Dataset` and `ShardPackSource` accept both expected IDs as 32-byte values:

```python
source = ShardPackSource(
    layout_key, config=config,
    expected_snapshot_id=bytes.fromhex(trusted_snapshot_hex),
    expected_layout_id=bytes.fromhex(trusted_layout_hex),
)
dataset = source.resolve()
```

Supply IDs independently of the untrusted CDN. Deriving them from a substituted
manifest does not authenticate it. Both IDs must be provided together; omitting
both trusts the initial transport. Identity does not authenticate timestamps
excluded from hashing or establish freshness without an externally selected ID.

`source.resolve()` fetches/validates once and retains the immutable native
dataset. Later epochs and `cache_plan()` reuse it. Construct a new source to
change versions; mutating URL/config fields after resolution does not reload it.
This is an in-process identity pin, **not a persistent GC job pin**.
Sources now contain process-local synchronization: create a source per process,
do not pickle it or run concurrent iterations on the same source.

## Cancellation and ownership

`records.cancellation_handle()` is frozen and thread-safe. `cancel()` wakes a
pending native read via a Tokio watch signal. Cancellation is terminal for that
reader and raises an error, never successful EOF. A partly advanced parser must
not be reused. Metrics handles remain usable after cancellation.

The reader itself remains thread-affine. `ShardPackSource` releases it on the
producer thread, including exception paths: tracebacks crossing to a consumer
must not retain and later destroy the native reader on another thread. Direct
native callers must follow the same owner-thread destruction rule.

`ThreadedLoader` cancels a direct ShardPackSource before joining its producer and
closes its iterator in producer cleanup. A local HTTP fault test withholds shard
responses and verifies cancellation from another thread wakes the pending read.

Limits still requiring work:

- Manifest resolution precedes creation of the cancellation handle. It is not
  interruptible through that handle.
- Arbitrary Python decode/collate callbacks cannot safely be force-killed;
  shutdown waits for running callbacks. This is not a universal bounded-time close.
- Generic iterable sources and SamplePoolSource wrappers do not yet forward the
  cancellation/epoch-planning protocol.
- Breaking a loop does not close a retained iterator automatically; explicitly
  close retained iterators when abandoning them.

## EpochPlan

Rust `EpochPlan::build` computes records and batches per rank with the same shard
assignment algorithm and seed as the reader. Count arithmetic is checked.
`require_equal_steps()` rejects zero or unequal step counts; it does not silently
pad, repeat or truncate data.

Python `Dataset.validate_epoch(...)` also rejects unequal final batch sizes
unless `drop_last=True`. Equal-step planning alone cannot justify unweighted
gradient averaging for unequal batch sizes.

For a direct ShardPackSource, ThreadedLoader validates each multi-rank epoch
using the actual scheduler-produced seed before reading shards/decoding.
`drop_last` does not bypass unequal-step checks. Raw record/source iteration
remains available without training constraints.

This is not a DDP coordination service: ranks must agree on identity, topology,
seed, batch settings and epoch. If one rank fails setup, the trainer still needs
to coordinate job cancellation.

## Private plaintext cache

On Unix, `plaintext/` is created with mode 0700; new shard and metadata files use
0600. Every open, including cache hits, rejects a symlink or a namespace with
group/other permissions. Existing permissive directories are not chmodded by
the library. Ciphertext caching is unchanged. Non-Unix plaintext caching fails
closed until an ACL implementation exists.

Migration: stop cache users, inspect ownership/contents, then deliberately
restrict the existing namespace or select a new private cache directory. No
automatic migration/deletion is performed. The cache root and ancestors must
remain trusted: this does not defend against root, hostile same-UID processes,
or adversarial replacement of ancestor directories.

## Remaining stages

1. Explicit epoch balancing/truncation/repetition policies with coverage reports,
   identity/topology-bound plans, Python node affinity and trainer-side agreement.
2. Cancellation/deadlines through manifest resolution and source wrappers;
   disk-full and interrupted-cache integration coverage.
3. Separate byte accounting for encoded admission, decoded results and final
   batches, with size estimation and an oversized-result policy. Arbitrary memory
   allocation inside user callbacks cannot be bounded by queue limits alone.
4. Python training checkpoints and persistent GC job pins bound to the resolved
   identity and epoch plan; avoid a second independent Python scheduler.
