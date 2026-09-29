# COCO cold-cache loader startup

2026-09-29, same machine and 8,192-image training selection as the
[YOLO alignment experiment](coco-yolo-alignment.md). This measures the **loader
alone**: JPEG decode, 640 square letterbox, annotation conversion, batch size 16,
four decode workers and pinned CPU output. No GPU transfer or model is included.

## Results

Three fresh process invocations per arm, ordered map/stream, stream/map,
map/stream. Each invocation performs a cold pass then a warm pass. Values below
are medians, independently computed per column.

| Metric | Map-style files / processes | ShardPack / threads |
| --- | ---: | ---: |
| Cold first batch | 3.192 s | 0.227 s |
| Cold full pass | 10.034 s | 4.110 s |
| Cold images/s | 816 | 1,993 |
| Warm first batch | 0.077 s | 0.201 s |
| Warm full pass | 4.486 s | 4.165 s |
| Warm images/s | 1,826 | 1,967 |
| Cold process-tree CPU seconds | 33.11 | 22.11 |
| Warm process-tree CPU seconds | 23.98 | 22.44 |
| Cold process-tree `read_bytes` | 1,349,996,544 | 1,338,585,088 |
| Warm process-tree `read_bytes` | 0 | 0 |

The complete cold loader is about **2.44×** faster and delivers the first batch
about **14×** sooner. This is **not** a container-only comparison: map-style
starts four Python `spawn` processes, while ShardPack starts threads. Startup,
I/O and initial pool fill are all included. Hot map-style reuses persistent
workers and produces its first batch sooner; streaming refills its 1,024-record
shuffle pool each iteration.

Warm throughput differs by about 7.7% in this small sample. Both loaders supply
far more than the roughly 344 images/s consumed by the earlier training run;
neither ratio implies an end-to-end training speedup.

## Cache protocol and limitations

- Before each cold pass, call `POSIX_FADV_DONTNEED` on the selected 8,192 JPEGs
  or this experiment's 17 `.spk` objects. The latter includes unused validation
  shards; only ten training shards are consumed.
- No global `drop_caches`, content writes, cgroup changes or memory pressure.
  Eviction is advisory, not permanent cache bypass. It can affect other users of
  these same dataset files, but not unrelated files.
- Eviction (0.11–0.13 s for JPEGs, 0.07–0.11 s for shards), imports, manifest
  loading and CUDA/pinned allocator initialization are outside timing. Timing
  starts just before `iter(loader)`. The eviction walk warms file metadata.
- Linux process I/O accounting covers parent and live workers, including worker
  startup/library reads. Cold reads approximately match dataset size; every warm
  pass reports zero block-storage reads. This supports successful page-cache
  eviction, not a claim that hardware caches were disabled.
- Every pass checks all 8,192 unique sample IDs and pinned output tensors.
- Preprocessing and worker count match, but process/thread execution, scheduling,
  prefetch depth and global/pool shuffle differ. Host activity is not isolated.
- A further storage-only comparison should prestart map workers and evict again;
  this startup test intentionally includes process creation.

## Reproduce

With the experiment prepared and `uv` groups `torch` and `yolo` enabled:

```bash
cd python
uv run --no-sync python benchmarks/coco_cold_loader.py map \
  --work /data/datasets/shardpack-yolo-alignment-20260929 --round 4
uv run --no-sync python benchmarks/coco_cold_loader.py stream \
  --work /data/datasets/shardpack-yolo-alignment-20260929 --round 4
```

Choose an unused round number; output files are not overwritten. Linux
`posix_fadvise` and CUDA-enabled pinning are required.
[Script](../../python/benchmarks/coco_cold_loader.py) and
[raw results](coco-cold-loader.json), including streaming stage timings, are
committed. Source images remain unchanged.
