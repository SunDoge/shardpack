# Plaintext cache: direct destination hashing

Measured on 2026-09-27 against baseline `9fe8e96`. The new verifier hashes
the caller's read destination directly rather than allocating and copying
each chunk through a stream adapter. A 1 MiB buffered file reader coalesces
small header reads and permits large reads to bypass its buffer.

| Operation | Before GiB/s | After GiB/s | Change |
|---|---:|---:|---:|
| Read verified cache in 1 MiB chunks | 2.893 | 3.604 | +24.6% |
| Read and iterate all records | 2.020 | 2.258 | +11.8% |

These are medians of five alternating pairs (AB, BA, AB, BA, AB). Observed
ranges do not overlap: chunks 2.85–2.96 vs 3.57–3.68 GiB/s; records 2.01–2.05
vs 2.22–2.31 GiB/s. This is a local microbenchmark, not a training speedup claim.

## Method

- Intel Core Ultra 9 285K, rustc 1.98.0, Linux x86_64, release thin LTO.
- Both processes pinned to CPU 0, frequency not locked. Same harness in both builds.
- Real local filesystem backend and LocalCache, 64 MiB of synthetic payload,
  1,024 records of 64 KiB, default 16 MiB blocks, codec None.
- AES-encrypted source populated and authenticated into a plaintext cache before
  timing. No network, decryption or cache construction is included in timing.
- Each operation warms up, then runs for at least one second. Throughput is
  logical payload GiB/s. Each iteration reopens the cache and reads through EOF.
- OS page cache is warm: these results are **not physical NVMe throughput**.
- Both versions verify the full plaintext BLAKE3 identity at EOF. Record mode
  additionally parses headers, verifies CRC, allocates block storage and iterates
  records. Logical content fingerprinting is disabled in both.
- JPEG decode, shuffle, augmentation, Python and model execution are not included.

## Safety checks

The new tests cover zero-length reads, one-byte reads, repeated EOF, prefilled
ReadBuf destinations, Pending polls, same-size corruption, truncation, extra data
and terminal I/O failures. A destination-pointer assertion verifies that the
hashing wrapper forwards the caller's buffer instead of allocating a temporary.
As before, whole-file identity is established only on successful EOF; early
consumer cancellation does not establish whole-file identity.

## Reproduce

Harness: `crates/shardpack-cache/benches/plaintext_read.rs`. Use the identical
harness and `[[bench]]` entry in the baseline and new checkouts:

```bash
cargo bench -p shardpack-cache --bench plaintext_read --no-run --locked
taskset -c 0 /path/to/baseline/plaintext_read
taskset -c 0 /path/to/new/plaintext_read
```

Alternate execution order for five pairs. Each invocation emits `operation,gib_s`.
[Raw measurements](plaintext-cache-read.csv) preserve all twenty observations.
