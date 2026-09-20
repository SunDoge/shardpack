# Compatibility policy

ShardPack has two independently versioned surfaces.

| Surface | Current | Reader guarantee | Writer behavior |
|---|---:|---|---|
| Binary shard container | 1, 2 | Reads plaintext v1 and AES-256-GCM v2; rejects unknown versions, algorithms and flags | Writes v1 by default; v2 only when encryption is explicit |
| Protobuf manifest package | `shardpack.v1` | Reads old v1 messages with absent optional fields and safely ignores unknown fields | Writes v1 using only assigned field numbers |
| Record codec/schema | Per snapshot | Determined by codec name, codec version, and schema ID | No implicit schema conversion |

Binary headers carry explicit format versions. Adding a codec does not change the
container version, but changing framing, checksum coverage, or directory entry
semantics does. Readers fail closed on unsupported versions.

Encrypted v2 requires an externally supplied key matching the layout manifest's
`encryption.key_id`. The key is an authorization secret, not a canonical-data
dependency: `SelfContained` still means no Lance rows, raw objects, annotation DB,
or other data is required once the caller is authorized. Key rotation is a physical
rewrite into another layout of the same snapshot and must preserve the content
fingerprint.

Manifest changes within `shardpack.v1` may only add fields with new numbers or enum
values. Existing field numbers and meanings are never reused. Optional fields added
after the initial release must define a safe interpretation when absent. A change
that cannot satisfy those rules requires a new protobuf package and canonical ID
domain.

Prost does not retain unknown fields when a message is decoded and re-encoded.
Published manifests are therefore copied as opaque bytes when mirroring; control
plane operations never decode and rewrite an existing immutable manifest.

Checked-in golden fixtures cover:

- exact v1 shard bytes;
- a pre-materialization-stats v1 snapshot manifest;
- a pre-materialization-stats v1 layout manifest.

The tests decode, validate, and reproduce these fixtures. This makes accidental
wire changes, canonical-ID changes, and loss of old optional-field defaults visible
in CI.

## Linux binary compatibility

The release CLI targets glibc 2.17 on x86-64 Linux. Zig and `cargo-zigbuild` are
versioned as project tools in `mise.toml`; install and run the compatibility check
with:

```bash
mise install
mise run test:glibc
```

The task builds the real `shardpack` release binary for
`x86_64-unknown-linux-gnu.2.17`, inspects its ELF version requirements, rejects any
`GLIBC_*` dependency newer than 2.17, and smoke-tests the resulting CLI. `readelf`
from GNU binutils is the only required system utility.
