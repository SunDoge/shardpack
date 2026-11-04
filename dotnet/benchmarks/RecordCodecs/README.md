# Record codec experiment

An isolated C# consumer benchmark; it does not change the ShardPack wire format
or add dependencies to the SDK. Schema and borrowing helper originate from the
local rxla `experiments/flatbuffers-media` experiment. Generated files are checked
in so running the benchmark does not require flatc.

From the repository root, on Linux x64 with .NET 10, Rust, and COCO val2017:

```sh
dotnet restore dotnet/benchmarks/RecordCodecs --locked-mode
dotnet run --project dotnet/benchmarks/RecordCodecs -c Release --no-restore -- \
  /data/datasets/coco2017 docs/benchmarks/record-codecs.json
dotnet run --project dotnet/benchmarks/RecordCodecs -c Release --no-build -- \
  /data/datasets/coco2017 docs/benchmarks/record-codecs-dense.json 1024
```

The optional last argument repeats the original annotation values to form a
synthetic metadata-heavy corpus; it is not a real COCO annotation distribution.
Every run selects the first 256 COCO validation images by numeric image ID.
Seven rounds alternate format order. Every operation is warmed before timing.
The harness checks every round-tripped field and JPEG byte, buffer aliasing,
truncation, logical pool length, record counts, and read checksums.

For correctness checks without timed rounds, prefix either run command with
`SHARDPACK_CODEC_VERIFY_ONLY=1`. This validates 768 records, compares owned and
reused encodings, checks blob ownership, and rejects truncated/bounded buffers.

All formats carry the same logical fields: ID, MIME, dimensions, float32 xywh
boxes, COCO category IDs, JPEG. FlatBuffers uses an array of box structs; Fory
uses primitive coordinate/label arrays. The metadata/raw layout is an explicit
application payload: uint32 little-endian metadata length, Fory frame, raw JPEG.
It is not a ShardPack format change or a Fory Row encoding.

`encode-owned` returns an independently owned byte array. `encode-reused` uses
one codec/builder and output buffer per worker; returned memory is borrowed until
the next encode. Fory's IBufferWriter overload still allocates internally in
1.7.7; its allocation is included. Do not compare the two encode modes as if
they promised identical ownership.

`read-header` reads ID, MIME, dimensions; `read-metadata` additionally traverses
every box/label; `read-blob` also returns a JPEG memory view and touches its ends.
The FlatBuffers verifier runs on **every** read. Fory object reads materialize
the complete object even for header-only access. `read-pooled-shard-*` adds a
real ShardPack/Rust/local filesystem path, warmed page cache, and pooled managed
copies. The `none`/`zstd` variants use block compression at level 1.

The preprocessing cases additionally decode JPEG to BGR and resize to 320x320
using OpenCV 5, with one native thread and one reused output Mat. The pooled
preprocessing case uses None compression. This is not a full augmentation,
tensor-transfer, GPU, or model-training benchmark. Managed allocations are
current-thread bytes, excluding Rust/OpenCV/native allocations. Each measure
starts after full GC; GC counts cannot represent a continuously running loader.

Regenerate accessors with the compiler version matching the NuGet runtime:

```sh
cd dotnet/benchmarks/RecordCodecs
mise trust
mise install flatc
mise run generate
```

The stock FlatBuffers NuGet runtime uses byte arrays. `BorrowedImage` supports
array-backed memory at offset zero and bounds verification to its **logical**
length, not the capacity of the pool bucket. It rejects arbitrary/nonzero-offset
memory. Keep the owning record alive until all borrowed data is consumed. The
benchmark does not silently copy unsupported memory to advertise zero-copy.

See [results and decision](../../../docs/benchmarks/record-codecs.md).
