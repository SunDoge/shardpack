# ShardPack binary format v1

For an implementation-oriented diagram, see
[How to parse a `.spk` shard](spk-parsing.md).

All integer fields are unsigned little-endian. Reserved bytes MUST be zero when
written and ignored when read. A reader MUST reject unknown format versions,
invalid sizes, checksum failures, truncated blocks, and record directories whose
payload sizes do not exactly consume the raw payload.

Reserved bytes may only gain semantics that older readers can safely ignore.
Changes to framing, decoding, or authentication require a new format version or
an explicitly supported codec/encryption identifier. Unknown shard/block flag bits
are rejected. Readers become terminal after a parsing error: subsequent reads fail
and no records or fingerprint updates from the failed block are exposed.

## Shard header (64 bytes)

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic: `SPKSHRD\0` |
| 8 | 2 | format version (`1`) |
| 10 | 2 | flags (bit 0 means encrypted) |
| 12 | 4 | header size (`64`) |
| 16 | 44 | reserved |
| 60 | 4 | CRC32C of bytes `[0, 60)` |

The header is followed immediately by zero or more blocks. There is no footer.

## Block header (64 bytes)

| Offset | Size | Field |
|---:|---:|---|
| 0 | 6 | magic: `SPKBLK` |
| 6 | 2 | block version (`1`) |
| 8 | 2 | flags; bit 0 marks a jumbo block |
| 10 | 4 | record count |
| 14 | 4 | directory bytes |
| 18 | 8 | stored body bytes |
| 26 | 8 | raw payload bytes |
| 34 | 1 | codec (`0=None`, `1=Zstd`) |
| 35 | 1 | encryption (`0=None`, `1=AES-256-GCM`) |
| 36 | 16 | per-shard key-derivation salt (zero when unencrypted) |
| 52 | 4 | sequential block index used as nonce counter (zero when unencrypted) |
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
a per-shard AES key with BLAKE3's KDF mode; the 32-bit block counter constructs a
nonce unique under that derived key. The counter's u32 width matches the u32 block
index width: a shard can contain at most `u32::MAX` blocks, with zero-based
indices `0..=u32::MAX - 1`. Writers and readers reject an additional block before
the count would overflow, so the counter never wraps or repeats within a shard.
Byte lengths and offsets are separately limited to `u64`. These integer bounds
describe format capacity, not an AES-GCM per-key security usage budget.
Every encrypted block repeats the same shard salt and its nonce
counter MUST equal its zero-based block index. Readers reject salt changes, gaps,
and repeated counters. The master key and key-management URI are never written
into a shard.

The exact key and nonce derivation is:

```text
key = BLAKE3.derive_key(
    context = "shardpack aes-256-gcm shard key v1",
    key_material = master_key[32] || salt[16])
nonce = zero[8] || big_endian_u32(block_index)
AAD = exact block_header[0..56]
```

Context strings are UTF-8 without a terminating NUL. The counter stored in the
header is little-endian; only the constructed AEAD nonce uses big-endian order.
The manifest and expected object identity must come from a trusted source. AEAD
authenticates blocks under the supplied key; full-object BLAKE3 checked at EOF
binds the stream to the pinned manifest. Records may be consumed before that final
check, so a failed identity check invalidates the read even if some records were
already delivered. Low-level block-boundary readers alone do not prove shard identity.

CRC32C is not a security primitive. For encrypted blocks, authenticity and
confidentiality come from AES-256-GCM; any wrong key or modified authenticated
bytes fail closed.

CRC32C uses a runtime-selected accelerated implementation: SSE4.2 on x86_64,
the CRC extension on little-endian AArch64, and a slice-by-8 portable fallback.
This is an implementation detail and does not change the v1 wire format.

## Record directory entry (28 bytes)

| Offset | Size | Field |
|---:|---:|---|
| 0 | 16 | stable SampleId |
| 16 | 8 | raw payload size, little-endian `u64` |
| 24 | 4 | flags, little-endian `u32` |

Record flags are opaque application/schema-defined bits, unlike shard/block flags.
All 32 bits are accepted and preserved; interpretation belongs to the record codec.
Readers must not skip records based on unknown record flags. Entries have no padding.

Payloads are concatenated in directory order. A record cannot cross a block. A
record whose payload plus its directory entry exceeds the configured block target
is emitted as a one-record jumbo block; block target size is therefore soft. The
writer counts both raw payload and the 28-byte entries toward the target, keeping
memory bounded even for empty or very small records.

The wire format can represent payload lengths up to `u64::MAX`. Implementations
MUST apply explicit resource limits before converting lengths to platform-sized
values or allocating memory.

## Identity and equivalence

`ShardId = BLAKE3(exact shard bytes)`. It includes every header, reserved byte,
directory, checksum and stored payload byte.

For logical content equivalence, each record digest is:

```text
BLAKE3("shardpack-record-v1" || sample_id || flags_le_u32 || raw_record_payload)
```

The digest contributes to two domain-separated 256-bit modular accumulators:

```text
a = little_endian_integer(digest)
b = little_endian_integer(BLAKE3("shardpack-fingerprint-b-v1" || digest))
accumulator_a = sum(a) mod 2^256
accumulator_b = sum(b) mod 2^256
```

Both accumulators start at zero and serialize as 32 little-endian bytes. Count is
the number of records including duplicates; an empty fingerprint has count zero
and two zero accumulators. Merging adds counts and accumulators (modulo applies
only to accumulators). This is an equivalence guard, not an adversarial content proof.
Together with record count this produces an order-independent multiset fingerprint.
Repacking, reordering, block-boundary changes and compression changes preserve it;
any SampleId, flag, or raw payload-byte change does not.

A sequential parser cannot distinguish a complete shard from one truncated exactly
at a block boundary. Layout consumers MUST therefore verify the manifest byte size
and `ShardId` before treating the object as complete. Per-block checksums remain the
fast recovery and transport-corruption checks.

## Resource safety

Readers apply configurable limits before allocating directory, stored-payload, or
raw-payload buffers. The default allows jumbo media records up to 8 GiB and at most
one million records per block. Deployments should lower these limits to match their
datasets.

The writer applies no matching record-per-block cap: with 28-byte directory entries,
a `target_block_bytes` above roughly 28 MiB can pack more than one million tiny
records into a single block that default reader limits reject. Writers targeting
such record densities must keep `target_block_bytes` within the limits their
consumers will use.

The authoritative v1 uncompressed golden vector lives at
`crates/shardpack-format/tests/data/v1_none.hex`. The encrypted vector is
`crates/shardpack-format/tests/data/v1_aes256gcm.hex`: master key is 32 bytes of
`0x11`, salt is 16 bytes of `0x22`, SampleId is 16 bytes of `0x33`, flags are
`0x12345678`, payload is ASCII `alpha`, codec is None, and block index is zero.
Its fingerprint count is 1, with accumulator bytes (hex):

```text
a = 6e33d2196d4e064ae74a3874eac66b89db705aa7d1b5eb98adf6ceba4742cc3b
b = 750a7a2985f6565c03377876e7fab7322af662b1ff91750c3dfbf84f11ace14c
```

The fixed salt/key are test data only; production writers generate a fresh salt.
The parser fuzz target is
`fuzz/fuzz_targets/parse_shard.rs`, `parse_encrypted_shard.rs`, and
`parse_manifest.rs` deliberately use bounded parser limits.

Encryption is optional and does not create another format version. Readers use the
shard flag and block encryption field to select the authenticated-decryption path;
unknown flags or algorithms are rejected rather than interpreted as plaintext.
