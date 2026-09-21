# Cloudflare and Backblaze B2 deployment

This deployment profile uses Backblaze B2 as the durable object store and
Cloudflare as the authenticated delivery and cache layer. It is an operational
optimization only: B2 remains the source of shard bytes, and Cloudflare cache
contents are never layout roots or durable copies.

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
client Range requests from that representation. When Workers Caching receives a
Range request, it can request the full response from the Worker, store it, and
slice the client-facing `206`. A Worker-generated `206 Partial Content` response
is not stored by Workers Caching.

Consequently, the default design is:

```text
GPU client Range GET
        |
        v
Cloudflare cached complete shard
        |
      miss
        v
Worker streams complete B2 object as 200
```

See [Workers cache configuration](https://developers.cloudflare.com/workers/cache/configuration/).
The Worker must pipe streams rather than calling `arrayBuffer()`, `bytes()`, or
`text()` on a shard.

If shards must remain larger than the cache limit, a future Worker may expose
synthetic, cacheable segment URLs and fill each segment with a B2 Range GET. Use
coarse 64--128 MiB segments rather than one request per 16 MiB block to avoid
excessive Worker and B2 operations. This is a fallback, not the default hot path.

## Identity, headers, and authorization

Shard paths must be content-addressed by `ShardId`. A successful immutable shard
response should provide equivalent semantics to:

```http
Cache-Control: public, max-age=31536000, immutable
ETag: "<ShardId>"
Content-Type: application/x-shardpack
Accept-Ranges: bytes
Content-Length: <exact shard byte size>
```

The Worker authenticates the caller before serving data. Temporary credentials,
signatures, and bearer tokens must not fragment the canonical cache key. B2
credentials remain Worker secrets and must never be forwarded to the client.

Cloudflare cache retention is popularity-based and may evict an object before
its freshness TTL expires. Therefore a cache hit is never assumed for
correctness, resume, or durability. See [cache retention versus freshness](https://developers.cloudflare.com/cache/concepts/retention-vs-freshness/).

## Cost model

Backblaze documents B2-to-Cloudflare delivery through the Bandwidth Alliance as
free of B2 download bandwidth charges. This applies to traffic delivered through
the Cloudflare path, including an edge miss that causes Cloudflare to contact B2.
Caching still reduces B2 API operations, origin load, and repeated-read latency.
It does not remove Cloudflare Worker/CDN charges or B2 request charges.

See [Backblaze's Cloudflare integration](https://www.backblaze.com/docs/cloud-storage-cloudflare-integrations/).

For training economics, the preferred hierarchy remains:

```text
local NVMe cache -> Cloudflare edge -> B2 origin
```

Cloudflare reduces origin transfer cost and can accelerate shared hot shards;
node-local NVMe remains the predictable cache for repeated epochs.
