# ShardPack binary format v1

For an implementation-oriented diagram, see
[How to parse a `.spk` shard](spk-parsing.md).

All integer fields are unsigned little-endian. Reserved bytes MUST be zero when
written and ignored when read. A reader MUST reject unknown format versions,
invalid sizes, checksum failures, truncated blocks, and record directories whose
payload sizes do not exactly consume the raw payload.

## Shard header (64 bytes)

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic: `SPKSHRD\0` |
| 8 | 2 | format version (`1`) |
| 10 | 2 | flags (bit 0 means encrypted) |
| 12 | 4 | header size (`64`) |
| 16 | 2 | layout fragment version |
| 18 | 42 | reserved |
| 60 | 4 | CRC32C of bytes `[0, 60)` |

The header is followed immediately by zero or more blocks. There is no footer.

## Block header (64 bytes)

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic: `SPKBLK\0\0` |
| 8 | 2 | block version (`1`) |
| 10 | 2 | flags; bit 0 marks a jumbo block |
| 12 | 4 | record count |
| 16 | 4 | directory bytes |
| 20 | 8 | stored body bytes |
| 28 | 8 | raw payload bytes |
| 36 | 1 | codec (`0=None`, `1=Zstd`) |
| 37 | 1 | encryption (`0=None`, `1=AES-256-GCM`) |
| 38 | 16 | per-shard key-derivation salt (zero for v1) |
| 54 | 2 | per-shard block nonce counter (zero for v1) |
| 56 | 4 | CRC32C of bytes `[0, 56)` |
| 60 | 4 | CRC32C of the complete physical block body |

The plaintext block body is `directory || stored_payload`; the directory is not
compressed. With encryption, that complete concatenation—including SampleIds,
sizes, flags, and compressed payload—is encrypted and followed by a 16-byte
authentication tag. `stored_body_bytes` always gives the complete physical body
length: plaintext bytes for an unencrypted block, or ciphertext plus tag for an
encrypted block. CRC32C covers that physical body, allowing fast transport-
corruption detection before authenticated decryption.

AES-GCM authenticates block-header bytes `[0, 56)` as associated data. A fresh
128-bit salt is generated per shard. The supplied 256-bit master key and salt derive
a per-shard AES key with BLAKE3's KDF mode; the 16-bit block counter constructs a
nonce unique under that derived key. An encrypted shard is therefore limited to
65,536 blocks (1 TiB with 16 MiB blocks), well above the recommended shard target.
The master key and key-management URI are never written into a shard.

CRC32C is not a security primitive. For encrypted blocks, authenticity and
confidentiality come from AES-256-GCM; any wrong key or modified authenticated
bytes fail closed.

CRC32C uses a runtime-selected accelerated implementation: SSE4.2 on x86_64,
the CRC extension on little-endian AArch64, and a slice-by-8 portable fallback.
This is an implementation detail and does not change the v1 wire format.

## Record directory entry (24 bytes)

| Offset | Size | Field |
|---:|---:|---|
| 0 | 16 | stable SampleId |
| 16 | 4 | raw payload size |
| 20 | 4 | flags |

Payloads are concatenated in directory order. A record cannot cross a block. A
record whose payload plus its directory entry exceeds the configured block target
is emitted as a one-record jumbo block; block target size is therefore soft. The
writer counts both raw payload and the 24-byte entries toward the target, keeping
memory bounded even for empty or very small records.

## Identity and equivalence

`ShardId = BLAKE3(exact shard bytes)`. It includes every header, reserved byte,
directory, checksum and stored payload byte.

For logical content equivalence, each record digest is:

```text
BLAKE3("shardpack-record-v1" || sample_id || raw_record_payload)
```

The digest contributes to two domain-separated 256-bit modular accumulators.
Together with record count this produces an order-independent multiset fingerprint.
Repacking, reordering, block-boundary changes and compression changes preserve it;
any SampleId or raw payload-byte change does not.

## Resource safety

Readers apply configurable limits before allocating directory, stored-payload, or
raw-payload buffers. The default allows jumbo media records up to 8 GiB and at most
one million records per block. Deployments should lower these limits to match their
datasets.

The authoritative v1 uncompressed golden vector lives at
`crates/shardpack-format/tests/data/v1_none.hex`. The parser fuzz target is
`fuzz/fuzz_targets/parse_shard.rs`, `parse_encrypted_shard.rs`, and
`parse_manifest.rs` deliberately use bounded parser limits.

Encryption is optional and does not create another format version. Readers use the
shard flag and block encryption field to select the authenticated-decryption path;
unknown flags or algorithms are rejected rather than interpreted as plaintext.
