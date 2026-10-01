# S3 prefetch: bounds, short reads and cancellation

2026-10-01. Follow-up to [full COCO training](coco-full-internal-s3.md).
This change improves the bounded input path; it does not change the format,
record ordering, shuffle algorithm, authentication or cache identity.

## Implementation changes

- Count the currently consumed reader within `network_concurrency`. Previously
  `take_next` refilled after popping, permitting N+1 readers and exceeding the
  configured aggregate chunk budget by one reader's share.
- Coalesce short reads into at most 1 MiB chunks with `BytesMut` / `read_buf`,
  avoiding explicit zero-initialization of every temporary chunk. Previously
  a small header read could hold an entire 1 MiB semaphore permit and channel
  slot, leaving much of the nominal prefetch budget unused. Filling a chunk
  trades some first-byte latency for useful read-ahead; EOF still delivers a
  partial final chunk, and errors are propagated rather than treated as EOF.
- Keep a task handle with each prefetched reader and abort it on drop. This
  stops background work when the consumer quits, even when the task is waiting
  on an object open/read rather than a channel send. Cancellation is asynchronous;
  underlying blocking OS operations are not forcibly interrupted.
- Add `ShardPackSource.cache_plan()` for read-only whole-layout budget/free-space
  planning. The full-cache training benchmark rejects undersized cache budgets
  before model setup. Intentional partial caching in the library remains valid.

The chunk budget is not a process RSS cap: HTTP buffers, current cache blocks,
decoded blocks, shuffle buffers and application tensors remain separate.

## Measurement protocol

`python/benchmarks/s3_prefetch.py` consumes the existing encrypted 8,192-record,
ten-shard internal-S3 COCO layout (1,338,564,602 stored bytes). Each invocation
creates a distinct empty plaintext cache, consumes one cold epoch and one warm
epoch, checks unique SampleIds/counts and reports native counters. It does not
decode JPEGs or run a model. The pool remains 1,024 records / 256 MiB, seed 42;
both epochs keep the same ordering. First-record latency includes manifest load,
connection setup and initial shuffle-pool fill, not just network first byte.

Runs execute sequentially, without deleting or modifying remote objects. The
initial proxy-routed connection failed before reading data; direct access via
`NO_PROXY` for the internal endpoint succeeded. Credentials are loaded through
`uv --env-file` and are not saved in results. Origin caches and shared network
load are uncontrolled; development tests/builds also ran on this shared host.
The old implementation was measured before rebuilding the native extension.

These are exploratory measurements, **not a randomized repeated old/new A/B
proof**, and must not be advertised as an equivalent training throughput gain.
New configurations vary both concurrency and total budget, keeping 32 MiB per
reader. The old nominal two-reader configuration actually allowed three.

## Results

| Implementation / config | Cold seconds | First record (s) | Cold records/s | Warm seconds |
| --- | ---: | ---: | ---: | ---: |
| Old, nominal 2 / 64 MiB | 61.690 | 17.453 | 132.79 | 0.926 |
| New, 2 / 64 MiB | 38.702 | 5.721 | 211.67 | 0.915 |
| New, 4 / 128 MiB | 39.161 | 7.751 | 209.19 | 0.970 |
| New, 8 / 256 MiB | 55.333 | 6.662 | 148.05 | 0.956 |
| New, 2 / 64 MiB repeat | 46.081 | 12.457 | 177.77 | 0.919 |

Rows are in execution order. All cold runs downloaded exactly 1,338,564,602 bytes
with ten misses; all warm runs had ten hits and zero shard downloads. There were
no retries, checksum errors, corrupt entries or evictions. Warm reads fit the
host page cache. Metadata requests still occur on warm epochs.

The new two-stream runs were faster than the single old baseline, but the repeat
also demonstrates material network variability. Four streams did not clearly
help and eight were slower. These observations support retaining conservative
defaults, not a universal speedup claim. The original 17-second first-record
wait is not completely eliminated; it includes filling the shuffle pool. We
did not weaken shuffle quality or bypass authentication to improve that number.

[Raw results](s3-prefetch.json) retain all counters. Five plaintext caches
(about 6.7 GB combined) and logs remain under
`/data/datasets/shardpack-prefetch-20261001-*`; an empty output directory from the
initial failed proxy attempt is retained too. Existing source keys/data were
unchanged, and no cleanup was performed.

## Reproduce

```bash
cd python
# Set NO_PROXY for the private endpoint if the host proxy cannot reach it.
uv run --no-sync --env-file /path/to/private.env python benchmarks/s3_prefetch.py \
  --source-spec /path/to/existing/source.json \
  --output /data/datasets/new-prefetch-run \
  --concurrency 2 --prefetch-mib 64
```

The output directory must not exist. This helper is for layouts up to 32 GiB
(its fixed cache budget); it retains all caches and measurement artifacts.
No new encryption keys, uploads or remote deletions are performed. The source
descriptor's existing key must still be available.

For an end-to-end training follow-up, `coco_yolo_alignment.py` now exposes
`--network-concurrency` and `--network-prefetch-bytes` alongside the cache budget.
Library defaults remain **two streams / 64 MiB**; the present measurements do
not justify raising everyone's memory and network use.

## Verification

Rust regressions cover the active-reader concurrency bound, fragmented input,
partial tails/EOF, propagation of partial-chunk errors and cancellation of a
stalled background task. Python tests cover cache-plan budget reporting, disabled
cache, insufficient disk estimates, no cache-directory creation and no metrics
mutation. Workspace Rust tests, 31 runtime tests, 64 Python tests, runtime Clippy,
Ruff and native API mypy checks passed.
