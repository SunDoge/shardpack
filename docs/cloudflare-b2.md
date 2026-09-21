# Cloudflare and Backblaze B2 deployment

This deployment profile stores ShardPack-authenticated ciphertext in a public
Backblaze B2 bucket and uses Cloudflare as a credential-free CDN. B2, Cloudflare,
other mirrors, and clients are outside the trust boundary; only holders of the
layout key can decode records. CDN contents are never layout roots or durable
copies.

## Object-size constraint

Cloudflare Workers can stream responses larger than their 128 MiB isolate memory
limit, and Cloudflare does not impose a Worker response-body size limit. The CDN
and Workers Cache API nevertheless limit an individual cacheable object to 512 MB
on Free, Pro, and Business plans; Enterprise permits larger objects. Cloudflare
also evaluates a Range request against the complete object's size, so requesting
a small range from a 1 GiB shard does not make that shard cacheable.

References:

- [Workers platform limits](https://developers.cloudflare.com/workers/platform/limits/)
- [Cloudflare Range request behavior](https://developers.cloudflare.com/cache/reference/range-requests/)
- [Workers streaming APIs](https://developers.cloudflare.com/workers/runtime-apis/streams/)

Use this layout profile when complete-shard edge caching is desired:

```toml
# Layout recipe values; these are writer/materializer options, not storage config.
target_shard_bytes = 402653184 # 384 MiB soft target
max_shard_bytes = 503316480    # 480 MiB hard ceiling
target_block_bytes = 16777216  # 16 MiB
```

The gap between 480 MiB and Cloudflare's advertised limit leaves room for unit
interpretation and future framing overhead. The materializer must close the
current shard before the next ordinary record would cross the hard ceiling. A
single record larger than the ceiling remains legal as a jumbo shard, but that
object is explicitly not CDN-cache-compatible.

The generic S3 profile may retain its approximately 1 GiB soft target. A
Cloudflare/B2 layout is another physical materialization of the same snapshot;
it does not require a new snapshot or a binary-format change.

The materializer records `max_shard_bytes` in the immutable layout recipe and
refuses to publish a non-jumbo shard that exceeds it. A hard limit should retain
substantial headroom above the soft target because headers, directories,
encryption tags, and compression framing are not part of the soft payload target.

## Range handling

Prefer a cacheable full-object `200 OK` response and let Cloudflare satisfy
client Range requests from that representation. A Range request does not make an
otherwise ineligible or oversized object cacheable.

Consequently, the default design is:

```text
GPU client GET / Range GET
        |
        v
Cloudflare cached complete shard
        |
      miss
        v
public B2 object
```

If shards must remain larger than the cache limit, an optional Worker may expose
synthetic, cacheable segment URLs and fill each segment with a B2 Range GET. Use
coarse 64--128 MiB segments rather than one request per 16 MiB block to avoid
excessive Worker and B2 operations. This is a fallback, not the default hot path.

## Identity, headers, and trust boundary

Shard paths must be content-addressed by `ShardId`. A successful immutable shard
response should provide equivalent semantics to:

```http
Cache-Control: public, max-age=31536000, immutable
ETag: "<ShardId>"
Content-Type: application/x-shardpack
Accept-Ranges: bytes
Content-Length: <exact shard byte size>
```

The public bucket contains only encrypted shards with content-addressed,
non-semantic paths. Upload, listing, mutation, and deletion remain authenticated;
only object download is public. The key and any sensitive manifest metadata are
distributed through a separate control plane. A private bucket plus an
authenticating Worker remains an optional policy choice, not a runtime
requirement.

The training config uses a read-only HTTP store pointed at the Cloudflare custom
domain:

```toml
[stores.training]
type = "http"
base_url = "https://data.example.com/shardpack"
```

This same interface accepts another CDN or an IPFS HTTP gateway. Exact ciphertext
BLAKE3 and block AEAD verification prevent an untrusted transport from silently
changing training data.

Cloudflare cache retention is popularity-based and may evict an object before
its freshness TTL expires. Therefore a cache hit is never assumed for
correctness, resume, or durability. See [cache retention versus freshness](https://developers.cloudflare.com/cache/concepts/retention-vs-freshness/).

## Cost model

Backblaze documents B2-to-Cloudflare delivery through the Bandwidth Alliance as
free of B2 download bandwidth charges. This applies to traffic delivered through
the Cloudflare path, including an edge miss that causes Cloudflare to contact B2.
Caching still reduces B2 API operations, origin load, and repeated-read latency.
It does not remove Cloudflare CDN costs or B2 request charges. Direct access to
the discoverable B2 origin can bypass Cloudflare and may incur download charges,
so use non-semantic content hashes, budget alerts, and provider-side limits. A
Worker/private origin is required if origin bypass itself is unacceptable.

See [Backblaze's Cloudflare integration](https://www.backblaze.com/docs/cloud-storage-cloudflare-integrations/).

For training economics, the preferred hierarchy remains:

```text
local NVMe cache -> Cloudflare edge -> B2 origin
```

Cloudflare reduces origin transfer cost and can accelerate shared hot shards;
node-local NVMe remains the predictable cache for repeated epochs. With
`cache_mode="plaintext"`, the first epoch trains while downloading and
authenticating ciphertext; later epochs read the derived plaintext shard only.
