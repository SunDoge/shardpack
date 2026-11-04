# C# FlatBuffers / Fory record experiment

Measured 2026-11-04. **For the present image-training use case, do not implement
C# Fory Row yet.** FlatBuffers already supplies low-allocation structured reads
and a portable schema/code-generation workflow. Existing Fory remains practical
when limited to small metadata with a separately borrowed blob. Whole-object
Fory with an embedded JPEG is substantially more allocation-heavy in the tested
C# implementation, but that does not establish a need for an entirely new Row
implementation.

This is a recommendation from the measurements below, not a measurement of
Fory Row: no C# Row implementation was available or simulated in this test.
Neither the ShardPack container nor its default payload codec was changed.

## Method and coverage

- Intel Core Ultra 9 285K, Debian 13, .NET runtime 10.0.14; one benchmark worker.
- Apache.Fory 1.7.7; flatc and Google.FlatBuffers runtime 25.2.10; OpenCV 5.0.0,
  package 5.0.0.20261018, explicitly limited to one native thread.
- 256 real COCO val2017 JPEGs sorted by numeric image ID: 41,010,144 encoded
  bytes, mean 160,196 B/image, 1,814 non-crowd boxes (mean 7.09/image).
- A second, **synthetic** dense-metadata corpus repeats original annotation
  values to make 1,024 boxes/image while retaining exactly the same JPEGs.
- All formats contain the same logical fields: image ID, MIME, dimensions,
  float32 xywh coordinates, COCO category IDs, JPEG bytes. FlatBuffers uses
  fixed-layout box structs; Fory uses coordinate and label arrays. Thus logical
  values match, but in-memory shapes and wire representations differ.
- Seven warmed rounds alternate format order. Tables show medians; the JSON
  retains every timing, allocation count, GC count, and checksum.
- Every FlatBuffers read invokes the generated verifier. Header-only access
  still includes structural verification; it does not traverse all box values.
- Allocations are managed bytes on the measurement thread, excluding native
  Rust/OpenCV memory. Each measure starts after full GC. This is deliberately
  not a long-running GC-pressure or retained-memory test.
- Pooled reads use actual ShardPack files through the C# SDK/Rust reader and
  local filesystem, warm page cache, default 16 MiB blocks, None or Zstd level 1.
  They include the native-to-managed pooled copy; they are not S3 measurements.
- JPEG processing decodes BGR and resizes to 320x320 with a reused output Mat.
  It does not include letterbox, augmentation, F32 tensor conversion, GPU
  transfer, model training, or parallel-worker scaling.

The harness checks every field and JPEG byte before timing, blob aliasing,
truncated FlatBuffers, and verification bounded by logical pool-record length.
Read checksums and record counts are checked across layouts. Source and commands:
[benchmark README](../../dotnet/benchmarks/RecordCodecs/README.md).
Raw results: [COCO](record-codecs.json), [dense metadata](record-codecs-dense.json).

## Ordinary COCO results

Each cell is **records/s / managed B per record**. `metadata/raw` means the
application payload `[u32 metadata length][Fory metadata frame][JPEG bytes]`,
not Fory Row and not a ShardPack format extension.

| Operation | FlatBuffers | Fory whole object | Fory metadata/raw |
|---|---:|---:|---:|
| Write, independently owned output | 32,505 / 160,461 | 32,082 / 160,488 | 36,949 / 160,593 |
| Write, reused output buffer | 129,072 / 0 | 29,561 / 160,488 | 107,205 / 196 |
| Read ID/MIME/dimensions only | 8,055,856 / 136 | 38,224 / 160,607 | 6,600,862 / 492 |
| Read all metadata | 3,407,158 / 136 | 37,046 / 160,607 | 6,037,291 / 492 |
| Read metadata + borrow/extract JPEG | 3,179,692 / 136 | 37,978 / 160,607 | 6,147,842 / 492 |
| Pooled ShardPack, None | 13,103 / 267 | 8,451 / 321,138 | 12,668 / 623 |
| Pooled ShardPack, Zstd | 6,237 / 267 | 4,843 / 321,138 | 6,046 / 623 |
| Payload read + JPEG decode/resize | 799 / 240 | 789 / 160,711 | 801 / 596 |
| Pooled None read + JPEG decode/resize | 742 / 372 | 721 / 321,242 | 743 / 728 |

FlatBuffers and metadata/raw have effectively equal image-processing throughput
here. Do not interpret differences of a few images/s as a demonstrated advantage.
The much larger allocation difference is directly measurable. At a hypothetical
1,000 records/s, the measured pooled whole-object Fory allocation rate would be
about 321 MB/s; this arithmetic is not a measured sustained-GC benchmark.

Fory metadata/raw is faster than verified FlatBuffers when traversing **all**
small metadata fields in these loops. A zero-copy representation does not
guarantee faster repeated accessor calls than traversing materialized arrays.

Total ordinary-corpus payload sizes are 41,071,888 B (FlatBuffers), 41,055,308 B
(Fory whole), and 41,054,541 B (metadata/raw). The size difference is insignificant
next to the JPEG corpus; it is not a useful reason to choose one here.

## Dense metadata: 1,024 boxes/image

This control isolates the cost of materializing arrays and reading selected
fields; it is not representative of normal COCO images.

| Operation | FlatBuffers | Fory whole object | Fory metadata/raw |
|---|---:|---:|---:|
| Read ID/MIME/dimensions only | 7,338,200 / 136 | 29,158 / 180,943 | 185,690 / 41,168 |
| Read all metadata | 70,278 / 136 | 27,044 / 180,943 | 120,817 / 41,168 |
| Write with reused output | 65,035 / 0 | 23,086 / 180,829 | 51,152 / 20,536 |
| Pooled None read | 10,228 / 267 | 7,212 / 361,815 | 10,940 / 41,299 |
| Pooled None read + JPEG decode/resize | 732 / 372 | 721 / 361,920 | 731 / 41,404 |

FlatBuffers benefits strongly when skipping box values: no array
materialization and constant managed allocation. Fory still reconstructs arrays
even if the application only wants dimensions. When traversing every box,
materialized Fory arrays again have higher loop throughput, at the cost of
allocations. JPEG processing continues to dominate this end-to-end CPU case.

## Why the Fory allocations occur

These are verified implementation properties of **1.7.7**, not fundamental wire
format limitations:

1. `Fory.Deserialize<T>(ReadOnlySpan<byte>)` calls `ByteReader.Reset(span)`,
   which creates a new array with `ToArray()`. The byte-array overload reuses the
   input array instead. A pooled record generally exposes a logical subrange of
   a larger array, so passing the entire bucket is not a valid workaround.
2. A decoded `byte[]` field uses `ReadBytes`, allocating and copying its output.
   A whole JPEG object therefore copies the JPEG even when input is an exact
   byte array. A bounded Span input additionally copies the entire frame.
3. `Fory.Serialize(IBufferWriter<byte>, value)` currently calls the owned-array
   `Serialize(value)` and then copies the result to the writer. Merely reusing
   `ArrayBufferWriter` does not remove that intermediate allocation.

See pinned source: [Fory.cs](https://github.com/apache/fory/blob/v1.7.7/csharp/src/Fory/Fory.cs),
[ByteBuffer.cs](https://github.com/apache/fory/blob/v1.7.7/csharp/src/Fory/ByteBuffer.cs).
This is why API improvements should be evaluated before committing to Row.

## Best practices

### FlatBuffers

- Use a table as the evolvable sample root. Put large encoded blobs in byte
  vectors and borrow them when reading. Use fixed-layout structs only for
  deliberately frozen data such as this Box; changing its layout is not an
  additive schema evolution.
- In C#, use generated buffer accessors in the reader hot path. Avoid
  `UnPack()`, `GetDataArray()`, or unconditional `ToArray()` when a view suffices.
- Keep the owning `PooledShardRecord` alive through image decoding/consumption.
  A `ReadOnlyMemory<byte>` view does not keep a pool lease valid after Dispose.
- With the stock 25.2.10 NuGet runtime, use the bounded array-backed adapter
  in this experiment. It supports offset-zero array memory, not arbitrary
  Memory/Span. Nonzero-offset or non-array inputs require an explicit adapter,
  copy, or different runtime configuration; do not silently ignore logical bounds.
- Keep generated verification enabled initially. CRC/AEAD authentication verifies
  byte integrity/authenticity, not whether a producer encoded valid FlatBuffers.
- Reuse one builder per worker. Consume borrowed output before `Clear()`/the
  next encode. This is useful with synchronous `ShardWriter.Write`; asynchronous
  channel handoff requires an owned copy or a lease until the consumer finishes.
- Pin flatc and runtime together. Keep `.fbs` sources and includes with the
  snapshot, export them for normal `flatc` generation, and record root type,
  identifier and size-prefix convention. No runtime reflection is needed for
  the generated-code path. This schema-shipping workflow remains a proposed SDK
  convenience, not a feature implemented by this benchmark.

### Fory

- Reuse a serializer **per worker**. This experiment uses registered types,
  primitive arrays, `TrackRef(false)`, and `Compatible(false)` because each
  snapshot has a fixed schema and independent records. Enable compatibility or
  reference tracking when required by the contract; their costs are not measured
  here. Do not share mutable serializer state across workers.
- For current C# APIs, keep large JPEG/audio/video blobs outside the Fory object
  when read allocation matters. The metadata/raw envelope here demonstrates one
  possible application codec. Document and version that envelope; it is not a
  universal standard or the default ShardPack payload format.
- Prefer exact-array deserialization when already available. Never pass pool
  padding as part of a frame merely to select a faster overload. Profile Span
  input copies explicitly.
- `IBufferWriter` is currently convenient, not allocation-free. Owned output
  remains simpler for asynchronous queues; borrowed output needs a clear lease.

## Decision on C# Fory Row

**Defer Row for ShardPack's present image pipeline.** Choose FlatBuffers when a
single portable, schema-bearing record with borrowed blob access is desirable.
Keep Fory where its object API is valuable and metadata is small. No production
codec migration is implied by these experiments.

If contributing to Fory, the lower-scope candidates are a bounded array-backed
`ReadOnlyMemory`/segment reader without the input copy, a writer that writes
directly into `IBufferWriter`, and explicit borrowed binary-field APIs. The last
needs an ownership contract; ordinary `byte[]` fields cannot simply become views.
These changes could improve Object APIs without introducing a second C# format.

Revisit Row when a real consumer needs to select a few fields out of large
structured records, dense annotations produce material GC pressure, or tensor
records make codec work significant relative to preprocessing. The dense test
shows this **access pattern** has potential value; it does not prove a future
Fory Row implementation will match FlatBuffers performance. Before implementing
Row, require a prototype to demonstrate a material reduction in complete loader
CPU/GC cost over both verified FlatBuffers and metadata/raw, while preserving
the preferred C# type-definition workflow and passing cross-language fixtures.

Relevant contracts: [FlatBuffers C#](https://flatbuffers.dev/languages/c_sharp/),
[schema evolution](https://flatbuffers.dev/evolution/),
[Fory format capabilities](https://fory.apache.org/docs/start/).
