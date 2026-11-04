# Multi-file schema bundles

A snapshot's existing `RecordSchemaDescriptor.embedded_schema` can hold a
portable directory of schema files, including FlatBuffers `include` dependencies.
The `.spk` shard framing is unchanged. A schema bundle is an optional encoding;
legacy single-file/opaque embedded schema bytes retain their existing meaning.

For example, given `schemas/detection.fbs` and `schemas/media/image.fbs`:

```sh
shardpack schema-pack schemas --entrypoint detection.fbs --output schema.bundle
# Prints schema_id=<64 hexadecimal digits>
```

Supply the printed ID and bundle when importing the shards. `--root` is an
OpenDAL filesystem store in this example; the same snapshot operations work on
configured object stores.

```sh
shardpack --root ./dataset import \
  --dataset-id example --codec-name flatbuffers --codec-version 1 \
  --schema-id <printed-schema-id> --embedded-schema-file schema.bundle \
  shards/example.spk
# Prints snapshot_object=<key>

shardpack --root ./dataset schema-export <snapshot-object-key> --output ./exported-schema
flatc --csharp -I ./exported-schema -o ./generated ./exported-schema/detection.fbs
```

Use `flatc --rust` or `--python` for those languages. Generate each included
schema too if its generated types are not supplied separately. FlatBuffers
compiler/runtime versions, root type, file identifier, size-prefix setting,
field semantics, and payload codec are the application's contract: this generic
bundle neither validates `.fbs` syntax nor proves that imports or payloads match.
Pin matching compiler/runtime versions and verify the exported schema with your
actual tools. Entry point and dependencies must be present in the supplied tree;
include resolution itself is checked by the schema compiler.

`schema-export` loads and validates the snapshot and embedded-schema digest,
requires an output directory that does not already exist, and prints its entry
point. Packing includes all regular files in the directory, so use a dedicated
schema directory. Symlinks and non-regular files are rejected. Paths are
slash-separated relative ASCII paths composed of letters, digits, `_`, `-`, and
`.`; empty, `.`/`..`, absolute, backslash, Windows-reserved names, trailing dots,
case-insensitive collisions, and file/directory conflicts are rejected. The
bundle is limited to 4,096 files and 16 MiB including encoding. Empty directories
are not represented. Export failure after directory creation may leave partial
files; remove the new output directory before retrying.

Wire representation: the bytes are `SPKSCHEMA` followed by byte `0x01`, then a
canonical protobuf `shardpack.v1.SchemaBundle` containing the entrypoint and
`SchemaFile` entries sorted by path. The schema ID is the existing
`BLAKE3("shardpack-record-schema-v1" || embedded_schema)`; it covers the entry
point, paths, and file contents. The implementation rejects unknown protobuf
fields and noncanonical encodings within this version. The bundle format is
codec-neutral; individual codecs determine the meaning of its files.

The descriptor makes schemas available with the snapshot, but does not provide a
model, augmentations, class mapping, or image semantics unless those are also
recorded. Snapshot manifests can contain sensitive schema details. Sharing an
individual `.spk` without the snapshot manifest does not carry the schema.
