# Optional encryption

ShardPack encryption is physical layout encryption. It does not change record
payload bytes after authenticated decryption, so encrypting, decrypting, or rotating
a key creates a new layout—not a new logical snapshot—and must preserve the layout
content fingerprint.

## Algorithm and key boundary

The first implementation uses block-level AES-256-GCM through `aws-lc-rs`.
`EncryptionKey` contains exactly 32 bytes, redacts `Debug`, and zeroizes its storage
when the last clone is dropped. A layout manifest stores only:

```text
algorithm = AES_256_GCM
key_id = caller-defined stable identifier
```

The caller resolves `key_id` through its KMS, secret manager, HSM, or offline key
store and injects key bytes into the writer/reader. ShardPack never persists key
material. A lost key makes the encrypted layout unrecoverable, so durable standalone
datasets require independently protected key backups.

Encryption covers both the record directory and compressed payload. Compression
happens before encryption. Each block is an independent AEAD/recovery unit, so
sequential streaming and block-boundary Range GET resume remain available.

## Throughput benchmark

Run the retained deployment-machine benchmark with:

```bash
cargo bench -p shardpack-format --bench decrypt_throughput
```

On the development x86_64 host, 16 MiB in-place block decryption measured:

```text
RustCrypto AES-256-GCM         2.89 GiB/s
RustCrypto ChaCha20-Poly1305   2.01 GiB/s
AWS-LC AES-256-GCM              9.49 GiB/s
AWS-LC ChaCha20-Poly1305        3.84 GiB/s
```

These are single-process microbenchmark results, not a cross-machine guarantee.
Re-run them on the intended GPU nodes, especially on ARM and machines without AES
acceleration. On this host AWS-LC AES is about 2.47 times as fast as AWS-LC
ChaCha20, so AES remains the only emitted algorithm for the MVP. The manifest and
block header retain an explicit algorithm field for a future portable alternative.
At 9.49 GiB/s, decryption is unlikely to bottleneck a single S3 stream or NVMe
reader; aggregate multi-worker throughput still needs end-to-end profiling.

## Public ciphertext and plaintext node cache

Encrypted, content-addressed shards may be distributed through a public object
bucket, CDN, mirror, or IPFS gateway. These systems are untrusted byte transports:
the runtime accepts a shard only when block authentication succeeds and the full
ciphertext matches the manifest `ShardId`. Encryption keys never appear in object
URLs, manifests, cache metadata, or CDN configuration.

The default local cache stores exact ciphertext. Trusted ephemeral GPU nodes may
select `plaintext` mode. On a cold miss the runtime authenticates each block,
removes encryption without decompressing its stored payload, and simultaneously
feeds training and writes a valid plaintext `.spk`. The entry is published only
after the complete remote ciphertext matches `ShardId`. Later epochs validate
and stream the local plaintext shard without AEAD work.

Plaintext entries are derived optimization artifacts, never layout objects or GC
roots. Cache metadata binds their local BLAKE3, length, and a domain-separated
one-way key commitment to the source encrypted `ShardId`; a warm hit still rejects
incorrect key material. Delete the cache when releasing a GPU host, or use an
encrypted volume when its disk is not an acceptable temporary trust boundary.
