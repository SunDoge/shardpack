# Runtime refactor: identity, lifecycle and node affinity

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
- Generic iterable sources require their own cancellation integration.
  SamplePoolSource now forwards cancellation, stop signals and batch/epoch
  validation, and closes its upstream iterator on exit.
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

For ShardPackSource, including SamplePoolSource wrappers, ThreadedLoader validates each multi-rank epoch
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
   identity/topology-bound plans and trainer-side agreement. Python node affinity
   is now connected; automatic balancing remains pending.
2. Cancellation/deadlines through manifest resolution and arbitrary sources;
   disk-full and interrupted-cache integration coverage.
3. Separate byte accounting for encoded admission, decoded results and final
   batches, with size estimation and an oversized-result policy. Arbitrary memory
   allocation inside user callbacks cannot be bounded by queue limits alone.
4. Python training checkpoints and persistent GC job pins bound to the resolved
   identity and epoch plan; avoid a second independent Python scheduler.

## Node affinity from Python

`NodeAffinity` is a frozen native object, not another Python shard scheduler:

```python
from shardpack import NodeAffinity, ShardPackSource, ThreadedLoader

affinity = NodeAffinity(
    node_id="gpu-node-a", node_ids=["gpu-node-a", "gpu-node-b"],
    local_rank=0, local_world_size=4, seed=42,
)
source = ShardPackSource(
    layout_key, config=config, affinity=affinity,
    cache_dir="/local/shared-node-cache",
    expected_snapshot_id=trusted_snapshot_id,
    expected_layout_id=trusted_layout_id,
)
loader = ThreadedLoader(source, decode=decode, batch_size=32)
```

All processes must agree on membership, layout, affinity seed and local world
size (homogeneous nodes in this API). Each process supplies its actual stable
node ID and local rank. The affinity seed is independent of epoch shuffle seeds.
Membership list order does not change ownership; membership changes can move
shards and require replanning.

Affinity replaces global-rank assignment: do not also pass rank/world_size to
ShardPackSource. RANK/WORLD_SIZE environment variables are ignored in affinity
mode. Direct `Dataset.records(affinity=...)` requires legacy rank/world/worker
options to remain at defaults. There is one source consumer per local rank,
with independent parallel decode threads.

Epochs shuffle shards within node ownership. Local ranks can exchange subsets
between epochs, so share a cache directory between ranks on the same trusted
node. A full warm cache needs capacity for that node's shard set; `cache_plan()`
still reports a conservative whole-layout estimate.

`EpochPlan::build_affinity` uses the reader's rendezvous ownership and ordering.
Its counts are ordered by sorted node ID, then local rank—not the membership
list order or the trainer's global rank numbering. Ownership is computed once
per shard, not repeatedly for each local rank.
`Dataset.validate_epoch(..., affinity=..., world_size=affinity.world_size)` checks
this plan. ThreadedLoader performs the check automatically using its epoch seed.

Affinity improves cache reuse, **not automatic balance**. Unequal ownership can
still fail the equal-step/tail-batch guard. No samples are silently repeated or
discarded. Raw source iteration remains available for inspection without these
training constraints.

Tests compare planner counts against every node/local-rank reader assignment,
verify complete non-overlapping coverage, reorder membership and advance epochs,
and confirm zero second-epoch shard downloads from node-local caches. A stalled
local HTTP test covers cancellation through SamplePoolSource and ThreadedLoader.
This is simulated topology coverage, not real multi-node NCCL training evidence.
