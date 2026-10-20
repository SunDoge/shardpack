# Finding training input bottlenecks

`ThreadedLoader` and `shardpack.pytorch.ThreadedDataLoader` accept
`enable_stats=True`, disabled by default. No new dependency, background logging
thread, exporter or CUDA synchronization is introduced.

```python
import json
from shardpack import ShardPackSource
from shardpack.pytorch import ThreadedDataLoader

source = ShardPackSource(layout_key, config=config, cache_dir="/local/cache")
loader = ThreadedDataLoader(
    source, decode=decode_sample, batch_size=64,
    decode_workers=4, enable_stats=True,
)
for step, batch in enumerate(loader):
    train_step(batch)
    if step % 100 == 0:
        print(json.dumps(loader.stats()))
print(loader.stats())  # remains available after EOF
print(source.stats())  # native counters, also available without loader timing
```

Collect on every distributed rank; rank 0 alone can hide an input straggler.
Counters contain no payloads, keys or credentials.

Live source counters use a shareable native `LoaderMetrics` handle backed by
atomic counters. Polling `source.stats()` or `loader.stats()` from the training
thread is safe while the producer is reading. The underlying `RecordLoader`
itself remains thread-affine: do not move it across threads. Direct native users
can obtain `records.metrics_handle()` on the reader's thread, then call the
handle's `snapshot()` from other threads or after the reader is dropped.
Fields are sampled independently, not as one atomic transaction.

## Snapshot contract

### Capacity and network preflight

Before starting a job, `source.cache_plan()` reads only layout metadata and
reports `stored_bytes`, `shard_count`, `cache_max_bytes`,
`budget_covers_stored_bytes`, `disk_free_bytes` and
`free_disk_covers_stored_bytes`. It does not create the cache directory, download
shards, advance the epoch or modify loader metrics. Disk fields are `None` when
no cache is configured (`cache_enabled=False`). For example:

The first call resolves and retains this source's immutable manifest; subsequent
calls and epochs reuse it. See [runtime refactor](runtime-refactor.md) for trusted
IDs and source lifecycle changes.

```python
plan = source.cache_plan()
if plan["cache_enabled"] and not plan["budget_covers_stored_bytes"]:
    print("Warning: cache budget cannot hold the full stored layout", plan)
```

This is a **whole-layout planning estimate**, not an exact per-node requirement
or a guarantee that the disk is sufficient. It does not subtract existing cache
entries; available space can change. Leave headroom for metadata, temporary
files and other users. Plaintext cache removes encryption overhead but retains
compression, so stored shard bytes are a conservative estimate of shard contents.
The report neither reserves disk space nor rejects intentional partial caches.

`network_concurrency` counts the active shard plus queued shard streams;
`network_prefetch_bytes` bounds their combined chunk buffers (not total process
RSS, HTTP internals, block/decode/shuffle allocations). Short reads are coalesced
into at most 1 MiB chunks. This improves buffer utilization but can delay the
first delivered bytes until one chunk or EOF is available. Dropping a prefetched
reader aborts its background task, including pending opens/reads; completion of
the abort is asynchronous, and any underlying blocking OS operation may finish.

Defaults remain two streams / 64 MiB. Tune both fields together using the
[S3 prefetch benchmark](benchmarks/s3-prefetch.md); more concurrent streams do
not guarantee a faster ordered record stream.

### Timing and counters

- Disabled: `{"enabled": false}`. No instrumentation clocks or locks are used.
- `status`: `not_started`, then `running`, then `completed`, `closed` (early
  iterator close), or `failed`.
- `elapsed_seconds` includes consumer training time between yields.
  `batches_delivered` counts delivered batches, not completed training steps.
- `timings`: per-stage `calls`, `items`, `total_seconds`, cumulative `mean_seconds`,
  and `p50_seconds` / `p95_seconds` from the **latest at most 256 observations**.
  `recent_samples` reports the window length. These are not whole-epoch quantiles.
- `queues`: approximate `current` and observed `peak` counts. Sampling can miss
  brief transitions. Current counts reset to zero on shutdown; peaks persist.
- `source`: Rust records/bytes, completed/verified shards, retries and checksum
  failures, plus cache counters when configured. Read-ahead means these may be
  ahead of consumer progress. Whole-shard verification is counted only at EOF.
- Snapshots are detached, JSON-serializable and safe to query concurrently.
  Native counters are sampled separately from Python timings. A new iteration
  resets timing statistics; only the latest started iteration is reported.
  Use separate loader/source objects for independent concurrent-iterator stats.

## Timing meanings

| Timing | Meaning |
|---|---|
| `batch_delivery` | Queue retrieval + consumer callback per delivered batch, excluding training between yields and initial iterator setup |
| `batch_wait` | Retrieval time after observing an empty output queue; includes terminal/error messages |
| `batch_backpressure` | Producer put time after observing a full output queue, including retries/cancellation and terminal messages |
| `source` | Pulling a chunk, including Rust I/O/decrypt/decompress/shuffle and EOF checks |
| `decode` | Worker execution per chunk, including augmentation in the callback; items counts attempted records |
| `decode_result_wait` | Getting the next ordered future, including small retrieval overhead even if ready; exposes head-of-line waiting |
| `collate` | Collation, including pinning in the PyTorch adapter |
| `consumer` | Caller-thread callback, including CPU-side H2D submission if configured |

Stage timings overlap and decode time sums across workers: **do not sum them to
get wall time or percentages**. Source/decode percentiles are per chunk, not per
record. Unused stages are absent. Timers finalize on exceptions too; attempted
work need not equal completed work in failed or cancelled iterations.

Queue counts are `batch_queue` (including transient terminal messages, capacity
`prefetch_batches`) and `decode_inflight_records` (submitted records whose ordered
future result has not been retrieved, capacity `decode_prefetch`). These are not
byte counts or the number of actively executing records. Arbitrary decoded
objects can alias memory: inspect RSS and configured native byte limits instead
of assuming these counts represent allocated memory.

This release does not separately time network/decrypt/decompress, collation
versus pinning, or GPU execution. A high combined source time is not proof of a
network problem.

## Interpretation

| Observation | Next check |
|---|---|
| Low delivery time / frequent producer backpressure | Loader is ahead of consumer; inspect model/H2D/collectives |
| High batch waits + high source time | Cache miss counters, remote throughput, local I/O, shuffle |
| High ordered-future waits, decode in-flight near capacity | Decode/augmentation, slow outlier chunks, worker count, nested library threads |
| High collate time | Tensor copies, batch construction, pinning |
| High consumer time | CPU-side callback/transfer submission; GPU completion is not measured |

Compare first-pass cache population, warm-cache training and prebuilt batches.
A warm LocalCache need not mean warm OS page cache. Record throughput, CPU, RSS
and first-batch latency too. Native byte-counter deltas over elapsed wall time
give source throughput; downloaded-byte counters distinguish cache misses.

Measure GPU execution/H2D separately with GPU-aware profiling or CUDA events.
Do not synchronize after every training step just to collect stats: that changes
overlap. A CPU timer around asynchronous `.to("cuda")` measures submission, not
transfer completion. User callbacks may themselves synchronize; this library
does not introduce such synchronization for instrumentation.

## Overhead benchmark

From `python/`, against **trusted local repository code**:

```bash
uv run --no-sync python benchmarks/loader_stats.py --baseline-ref 9c5832b
```

Rotates baseline/disabled/enabled order for five rounds, warms each case and
times at least one second per case. Four workers decode 64 KiB zlib payloads in
GIL-releasing native code, with 64-record batches, 16-record tasks and 128
prefetched records. This measures instrumentation overhead, not storage or GPU
training. Tiny Python callbacks or single-record tasks may incur larger relative
overhead. Keep timings off for normal runs unless collecting diagnostics.

Measured on 2026-09-29, Core Ultra 9 285K / Python 3.14, with no CPU affinity or
frequency lock: median baseline 107,731 records/s, disabled 105,817 (-1.8%),
enabled 101,861 (-3.7% versus disabled). Baseline/disabled ranges overlap. Treat
these as directional observations on a very fast synthetic decode workload,
not an overhead guarantee for real training. No storage, CUDA or model work is
included. [Raw observations](benchmarks/loader-stats.jsonl).
