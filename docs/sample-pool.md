# Companion sampling for Mosaic / MixUp

`SamplePool` is a Rust runtime primitive with a Python binding. It holds a bounded
recent history of encoded records and samples companions **without removing them**.
It is separate from shuffle, does not decode payloads and performs no I/O. No
file-format, manifest, dependency or primary-epoch semantics change is required.

## Group before parallel decode

```python
from shardpack import SamplePoolSource, ShardPackSource
from shardpack.pytorch import ThreadedDataLoader

primary = ShardPackSource(
    layout_key, config=config, cache_dir="/local/cache",
    shuffle_max_records=1024, shuffle_max_bytes=256 * 1024**2,
)
groups = SamplePoolSource(
    primary,
    extra_samples=3,
    max_records=1024,
    max_bytes=256 * 1024**2,
    seed=42,
    replace=False,
    warmup="skip",
)

def decode_group(group):
    sample = decode_sample(group.primary)  # application-defined payload codec
    if not group.extras:
        return augment_single(sample)
    companions = [decode_sample(record) for record in group.extras]
    return apply_mosaic([sample, *companions])  # application augmentation/bbox logic

loader = ThreadedDataLoader(groups, decode=decode_group, batch_size=64,
                           decode_workers=4, enable_stats=True)
```

`SampleGroup` has `primary: Record` and `extras: tuple[Record, ...]`. Single-image
and Mosaic callbacks must produce compatible shapes/types for collation.
Mosaic image placement, crop/resize, class/bbox/mask transformations and random
augmentation parameters are deliberately **not** implemented in the container.
See `python/tests/test_sample_pool.py` for a real encoded-image packing, threaded
decode and four-image tiling test (not a full detection augmentation algorithm).

The wrapper selects companions before inserting the current primary, excludes
its SampleId (including previous occurrences), then yields exactly one group per
primary. No lookahead, extra remote GETs or additional primary consumption occurs.
It runs in the loader's source producer, before dispatching groups to workers.
The original primary payload is not replaced by the pool's compact copy.
Use ShardPackSource for threaded loading: it constructs the native reader inside
the producer thread. A native `Dataset.records()` iterator is thread-affine and
must not be created on one thread and consumed on another. It is fine for direct
same-thread iteration; a materialized list of immutable Records can be shared.

## Warmup and replacement

- Default `warmup="skip"`: if there are too few eligible companions, `extras` is
  empty; the primary is still yielded, so only Mosaic is skipped, not the sample.
- `warmup="replace"`: draw with replacement while history is insufficient.
  With one eligible record, all three companions may refer to it. With no eligible
  history, extras is still empty. Once enough history exists, `replace` applies.
- `replace=True`: replacement is allowed even with enough history. The primary
  is always excluded. Default false returns distinct SampleIds.
- `extra_samples=0` yields groups with no extras. Limits must still be positive.

## Manual pool API

```python
from shardpack import SamplePool

pool = SamplePool(max_records=1024, max_bytes=256 * 1024**2, seed=42)
pool.insert(record)
eligible = pool.available(exclude_ids=[record.sample_id])
if eligible >= 3:
    extras = pool.sample(3, exclude_ids=[record.sample_id], replace=False)
print(len(pool), pool.buffered_bytes)
pool.reset(seed=43)
```

Rust exposes `SamplePool`, `SamplePoolLimits` and `SamplePoolError` from
`shardpack-runtime`, including when default features are disabled. Python errors
for invalid limits, oversized payloads, invalid exclusions or insufficient
history are ValueError (integer conversion can additionally raise OverflowError).
SampleIds must be exactly 16 bytes. `count=0` returns an empty list without RNG
consumption. Failed sample/insert operations leave pool/RNG state unchanged.
To bound individual result lists, count is capped at `max_records`, including
with replacement; the wrapper therefore requires `extra_samples <= max_records`.

## Ownership, bounds and eviction

- FIFO by insertion/refresh time, **not** LRU-by-sampling. Sampling does not affect
  eviction. Duplicate SampleId replaces the older record and refreshes its age.
- Insert compacts the payload once, avoiding a small slice pinning a whole block.
  Eviction satisfies both byte and record limits; oversized insertion fails before
  mutation rather than silently skipping data or exceeding the limit.
- Sampling clones compact Bytes references, not payloads. Returned records and
  readonly memoryviews survive eviction, reset and pool destruction.
- The byte limit covers pool-owned encoded payload storage, excluding metadata,
  transient incoming copies, upstream shuffle and downstream-held sampled handles.
  Holding sampled outputs can extend memory lifetime beyond the pool's limit.
- Encoded records can require repeated decode. This API is not a decoded-image
  cache. A decode task containing one group may decode four images; account for
  this when setting decode-prefetch/task size and interpreting loader timings.
  Native counters and primary coverage do not count companion reuse as new reads.

## Reproducibility and statistical scope

Each pool owns its RNG, domain-separated from shuffle. The same seed and ordered
sequence of insert/sample operations gives the same companion selection. The
source wrapper creates a fresh pool for each iteration. Set `groups.seed`
explicitly before an epoch if epoch-varying sampling is wanted; it does not
automatically change upstream epoch or shuffle seeds.

Do not call a shared mutable pool from decode workers if order-independent
reproducibility is required. The wrapper selects groups on one producer, making
selection independent of decode-worker scheduling. Actual augmentation randomness
remains the caller's responsibility. Changing source order, rank assignment or
limits changes the selections. There is no SamplePool checkpoint serialization;
exact mid-epoch reproduction requires replaying its ordered input/operations.

Companions come only from the current rank's recent history, not uniformly from
the complete dataset. Offline/shard/pool shuffle can help avoid strongly
correlated history. Never mix train and validation records in one pool. This
feature alone does not establish Mosaic's model-quality effects or guarantee
training throughput improvements.

`SamplePoolSource.stats()` forwards upstream counters when available, and
`ThreadedLoader.stats()["source"]` includes them for this wrapper. Source-stage
time now includes companion selection/compaction. Decode item counts represent
groups, not the total number of primary-plus-companion image decodes.
