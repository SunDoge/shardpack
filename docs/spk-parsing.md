# How to parse a `.spk` shard

This is the visual companion to the normative [binary format](binary-format.md).
All integers are unsigned little-endian. Parsing is strictly sequential: no
footer, index, or seek is required.

## Physical byte stream

```mermaid
flowchart LR
    file([".spk byte stream"])
    shard["ShardHeader<br/><b>64 bytes</b><br/>magic = SPKSHRD\\0"]
    bh1["BlockHeader 0<br/><b>64 bytes</b>"]
    body1["BlockBody 0<br/><b>stored_body_bytes</b>"]
    bh2["BlockHeader 1<br/><b>64 bytes</b>"]
    body2["BlockBody 1<br/><b>stored_body_bytes</b>"]
    more["… repeat until clean EOF"]

    file --> shard --> bh1 --> body1 --> bh2 --> body2 --> more

    classDef fixed fill:#e8f0ff,color:#153663,stroke:#668bc2;
    classDef variable fill:#e9f7ed,color:#164426,stroke:#62a276;
    class shard,bh1,bh2 fixed;
    class body1,body2 variable;
```

EOF is valid only between blocks. EOF inside a 64-byte header or block body is a
truncated shard. `ShardId` is BLAKE3 over this entire exact byte stream.

## Fixed headers

```mermaid
flowchart TB
    subgraph SH["ShardHeader — 64 bytes"]
        direction LR
        sh0["0..8<br/>magic"]
        sh1["8..10<br/>version"]
        sh2["10..12<br/>flags"]
        sh3["12..16<br/>header_size = 64"]
        sh4["16..60<br/>reserved = 0"]
        sh6["60..64<br/>CRC32C(0..60)"]
        sh0 --> sh1 --> sh2 --> sh3 --> sh4 --> sh6
    end

    subgraph BH["BlockHeader — 64 bytes"]
        direction LR
        bh0["0..8<br/>magic"]
        bh1["8..10<br/>version"]
        bh2["10..12<br/>flags"]
        bh3["12..16<br/>record_count"]
        bh4["16..20<br/>directory_bytes"]
        bh5["20..28<br/>stored_body_bytes"]
        bh6["28..36<br/>raw_payload_bytes"]
        bh7["36<br/>codec"]
        bh8["37<br/>encryption"]
        bh9["38..54<br/>key salt"]
        bh10["54..56<br/>nonce counter"]
        bh11["56..60<br/>CRC32C(0..56)"]
        bh12["60..64<br/>body CRC32C"]
        bh0 --> bh1 --> bh2 --> bh3 --> bh4 --> bh5 --> bh6 --> bh7 --> bh8 --> bh9 --> bh10 --> bh11 --> bh12
    end

    SH --> BH

    classDef checksum fill:#fff0d8,color:#4d3500,stroke:#bd8425;
    class sh6,bh11,bh12 checksum;
```

Shard and block version are always `1`. Shard flag bit 0 selects encryption.
Block codec `0` is none and `1` is Zstd. Block encryption `0` is none and `1` is
AES-256-GCM. Block flag bit 0 means a one-record jumbo block.

## Decode one block

```mermaid
flowchart TD
    start(["Read exactly 64 bytes"])
    header["Decode BlockHeader<br/>validate magic, version, flags and limits"]
    hcrc{"CRC32C(header[0..56])<br/>matches bytes 56..60?"}
    readbody["Read exactly stored_body_bytes"]
    bcrc{"CRC32C(physical body)<br/>matches bytes 60..64?"}
    derive["Derive per-shard key<br/>master key + key_salt"]
    decrypt["Nonce = 10 zero bytes +<br/>nonce_counter as big-endian u16<br/>AAD = header[0..56]<br/>decrypt and remove 16-byte tag"]
    plain["Plain body"]
    split["Split at directory_bytes<br/>directory | stored payload"]
    directory["Parse record_count entries<br/>28 bytes each"]
    codec{"codec?"}
    none["Use stored payload directly"]
    zstd["Zstd-decompress to exactly<br/>raw_payload_bytes"]
    records["Slice raw payload in directory order<br/>using each payload_size"]
    exact{"Sizes consume raw payload<br/>exactly?"}
    emit(["Emit sample_id, flags, opaque payload"])
    fail(["Reject shard"])

    start --> header --> hcrc
    hcrc -- no --> fail
    hcrc -- yes --> readbody --> bcrc
    bcrc -- no --> fail
    bcrc -- yes --> encrypted2{"encrypted?"}
    encrypted2 -- yes --> derive --> decrypt --> plain
    encrypted2 -- no --> plain
    plain --> split --> directory --> codec
    codec -- none --> none --> records
    codec -- zstd --> zstd --> records
    records --> exact
    exact -- no --> fail
    exact -- yes --> emit

    classDef failure fill:#ffe7e7,color:#681b1b,stroke:#bd5555;
    classDef output fill:#e9f7ed,color:#164426,stroke:#62a276;
    class fail failure;
    class emit output;
```

For plaintext blocks, the physical body is:

```text
directory[directory_bytes] || stored_payload
```

For encrypted blocks, the physical body is:

```text
AES-GCM(directory || stored_payload) || authentication_tag[16]
```

`stored_body_bytes` is always the length of the complete physical body. In the
encrypted case it includes the authentication tag. After optional decryption,
split the plaintext at `directory_bytes`.

## Directory to records

```mermaid
flowchart LR
    subgraph D["RecordDirectory — record_count × 28 bytes"]
        e0["Entry 0<br/>sample_id[16]<br/>payload_size u64<br/>flags u32"]
        e1["Entry 1<br/>sample_id[16]<br/>payload_size u64<br/>flags u32"]
        en["Entry N-1<br/>sample_id[16]<br/>payload_size u64<br/>flags u32"]
        e0 --> e1 --> en
    end

    subgraph P["Raw payload — raw_payload_bytes"]
        p0["Record 0 bytes<br/>size = entry 0.payload_size"]
        p1["Record 1 bytes<br/>size = entry 1.payload_size"]
        pn["Record N-1 bytes<br/>size = entry N-1.payload_size"]
        p0 --> p1 --> pn
    end

    e0 -. maps to .-> p0
    e1 -. maps to .-> p1
    en -. maps to .-> pn
```

Required consistency checks:

- every block's version/encryption mode agrees with the shard header;
- `directory_bytes == record_count * 28`;
- `record_count > 0`;
- a jumbo block has exactly one record;
- the sum of all `payload_size` values equals `raw_payload_bytes`;
- decompression produces exactly `raw_payload_bytes`;
- an encrypted block has a 16-byte tag and valid authentication;
- encrypted blocks use one shard salt and `nonce_counter == zero-based block index`;
- after the last complete block, the next read must be a clean EOF.

A clean EOF only proves that the stream ended on a block boundary. Verify the
manifest `byte_size` and full-object `ShardId` to detect loss of complete trailing
blocks.
