using System.Runtime.CompilerServices;
using System.Buffers;
using SunDoge.ShardPack;

static class DataLoaderChecks
{
    private sealed class Owned(int value, Action release) : IDisposable
    {
        internal int Value => value;
        private int disposed;
        public void Dispose() { if (Interlocked.Exchange(ref disposed, 1) == 0) release(); }
    }
    private static void Check(bool condition) { if (!condition) throw new Exception("DataLoader check failed."); }
    internal static async Task Run()
    {
        int reads = 0, decoded = 0, released = 0, batches = 0, freedBatches = 0, active = 0, peak = 0;
        IEnumerable<ShardRecord> Source(CancellationToken token)
        {
            try
            {
                for (int i = 0; i < 23; i++) { token.ThrowIfCancellationRequested(); Interlocked.Increment(ref reads); yield return new(new byte[16], (uint)i, [(byte)i]); }
            }
            finally { Interlocked.Decrement(ref active); }
        }
        IEnumerable<ShardRecord> Tracked(CancellationToken token) { Interlocked.Increment(ref active); return Source(token); }
        var options = new DataLoaderOptions { BatchSize = 4, DecodeWorkers = 3, PrefetchRecords = 5, PrefetchBatches = 2, ShuffleCapacity = 4, Seed = 23, DropLast = false };
        var loader = new DataLoader<Owned, Owned[]>(Tracked, (record, token) => {
            int working = Interlocked.Increment(ref decoded); int previous;
            do { previous = Volatile.Read(ref peak); if (working <= previous) break; } while (Interlocked.CompareExchange(ref peak, working, previous) != previous);
            try { token.WaitHandle.WaitOne((23 - (int)record.Flags) % 3); token.ThrowIfCancellationRequested(); return new((int)record.Flags, () => Interlocked.Increment(ref released)); }
            finally { Interlocked.Decrement(ref decoded); }
        }, (samples, _) => { Interlocked.Increment(ref batches); return samples.Select(x => new Owned(x.Value, () => Interlocked.Increment(ref freedBatches))).ToArray(); }, options);
        async Task<int[]> Collect()
        {
            var result = new List<int>();
            await foreach (var batch in loader.ReadEpochAsync()) { foreach (var item in batch) { result.Add(item.Value); item.Dispose(); } }
            return result.ToArray();
        }
        var first = await Collect(); var second = await Collect();
        Check(first.SequenceEqual(second) && first.Order().SequenceEqual(Enumerable.Range(0, 23)) && peak > 1 && peak <= 3 && active == 0 && released == 46 && freedBatches == 46 && batches == 12);
        Console.WriteLine("PASS: bounded parallel decode preserves seeded sample order, owns decoded samples, handles tail batches and closes sources");

        using (var unblock = new ManualResetEventSlim())
        {
            int sourceCount = 0;
            var reachedBound = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var ordered = new DataLoader<int, int>(token => Enumerable.Range(0, 1000).Select(i => {
                if (Interlocked.Increment(ref sourceCount) == 5) reachedBound.TrySetResult();
                return new ShardRecord(new byte[16], (uint)i, []);
            }), (record, token) => { if (record.Flags == 0) unblock.Wait(token); return (int)record.Flags; }, (samples, _) => samples[0],
                new() { DecodeWorkers = 2, PrefetchRecords = 3, PrefetchBatches = 1 });
            await using var iterator = ordered.ReadEpochAsync().GetAsyncEnumerator();
            var firstBatch = iterator.MoveNextAsync().AsTask();
            await reachedBound.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Check(sourceCount <= 5 && !firstBatch.IsCompleted);
            unblock.Set(); Check(await firstBatch && iterator.Current == 0);
        }
        Console.WriteLine("PASS: a stalled early decode cannot grow the reorder buffer beyond the global in-flight bound");

        int sampleCreated = 0, sampleFreed = 0, batchCreated = 0, batchFreed = 0, count = 0;
        var bounded = new DataLoader<Owned, Owned>(token => Enumerable.Range(0, 1000).Select(i => { token.ThrowIfCancellationRequested(); Interlocked.Increment(ref count); return new ShardRecord(new byte[16], (uint)i, []); }),
            (record, _) => { Interlocked.Increment(ref sampleCreated); return new((int)record.Flags, () => Interlocked.Increment(ref sampleFreed)); },
            (samples, _) => { Interlocked.Increment(ref batchCreated); return new(samples[0].Value, () => Interlocked.Increment(ref batchFreed)); },
            new() { BatchSize = 1, DecodeWorkers = 2, PrefetchRecords = 3, PrefetchBatches = 1 });
        await foreach (var batch in bounded.ReadEpochAsync())
        {
            using (batch) { await Task.Delay(30); Check(count < 20); }
            break;
        }
        Check(sampleCreated == sampleFreed && batchCreated == batchFreed);
        Console.WriteLine("PASS: slow consumers apply backpressure; early exit drains workers and disposes queued samples/batches");

        using (var canceled = new CancellationTokenSource())
        {
            try
            {
                await foreach (var batch in bounded.ReadEpochAsync(cancellationToken: canceled.Token)) { batch.Dispose(); canceled.Cancel(); }
                throw new Exception("Cancellation not propagated.");
            }
            catch (OperationCanceledException) when (canceled.IsCancellationRequested) { }
        }
        Check(sampleCreated == sampleFreed && batchCreated == batchFreed);
        var failing = new DataLoader<int, int>(_ => Enumerable.Range(0, 100).Select(i => new ShardRecord(new byte[16], (uint)i, [])),
            (record, _) => record.Flags == 2 ? throw new IOException("decode sentinel") : (int)record.Flags,
            (samples, _) => samples[0], new() { PrefetchRecords = 3 });
        try { await foreach (var _ in failing.ReadEpochAsync()) { } throw new Exception("Decode failure lost."); }
        catch (IOException error) when (error.Message == "decode sentinel") { }
        Console.WriteLine("PASS: cancellation and decoder exceptions propagate while cleaning up outstanding ownership");

        using var memoryPool = new CountedPool();
        using var stream = new MemoryStream();
        using (var writer = new ShardWriter(stream, new() { Codec = BlockCodec.Zstd }, leaveOpen: true))
        {
            for (int i = 0; i < 16; i++) writer.Write(new byte[16], Enumerable.Repeat((byte)i, 70000).ToArray(), (uint)i);
            writer.Finish();
        }
        byte[] encodedShard = stream.ToArray();
        using (var reader = new ShardReader(new MemoryStream(encodedShard)))
        {
            using var firstRecord = reader.ReadPooled(memoryPool)!;
            using var secondRecord = reader.ReadPooled(memoryPool)!;
            Check(firstRecord.Payload.Span[0] == 0 && secondRecord.Payload.Span[0] == 1 && memoryPool.Active == 2);
        }
        Check(memoryPool.Active == 0);
        IEnumerable<IShardRecord> Pooled(CancellationToken token)
        {
            using var reader = new ShardReader(new MemoryStream(encodedShard));
            while (reader.ReadPooled(memoryPool) is { } record) yield return record;
        }
        var leased = new DataLoader<int, Owned>(Pooled, (record, _) => record.Payload.Span[0],
            (samples, _) => new Owned(samples[0], () => { }),
            new() { DecodeWorkers = 2, PrefetchRecords = 3, PrefetchBatches = 1, ShuffleCapacity = 4 });
        await foreach (var item in leased.ReadEpochAsync()) { item.Dispose(); break; }
        Check(memoryPool.Active == 0);
        var pooledFailure = new DataLoader<int, int>(Pooled, (record, _) => throw new IOException("pooled sentinel"), (samples, _) => samples[0]);
        try { await foreach (var _ in pooledFailure.ReadEpochAsync()) { } throw new Exception("Pooled decoder failure lost."); }
        catch (IOException error) when (error.Message == "pooled sentinel") { }
        Check(memoryPool.Active == 0);
        Console.WriteLine("PASS: pooled native record copies survive subsequent reads and return every lease on early exit or decode failure");

        int prepared = 0, disposed = 0;
        async IAsyncEnumerable<Owned> Prepare([EnumeratorCancellation] CancellationToken token = default)
        {
            for (int i = 0; i < 100; i++) { token.ThrowIfCancellationRequested(); await Task.Yield(); Interlocked.Increment(ref prepared); yield return new(i, () => Interlocked.Increment(ref disposed)); }
        }
        await foreach (var item in Prepare().Prefetch(1))
        {
            using (item) { await Task.Delay(20); Check(prepared <= 2); }
            break;
        }
        Check(prepared == disposed);
        Console.WriteLine("PASS: async stage prefetch bounds in-flight preparation and releases abandoned owned items");
    }
    private sealed class CountedPool : MemoryPool<byte>
    {
        internal int Active;
        public override int MaxBufferSize => int.MaxValue;
        public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
        { Interlocked.Increment(ref Active); return new Owner(this, MemoryPool<byte>.Shared.Rent(minBufferSize)); }
        protected override void Dispose(bool disposing) { Check(Active == 0); }
        private sealed class Owner(CountedPool pool, IMemoryOwner<byte> owner) : IMemoryOwner<byte>
        {
            private IMemoryOwner<byte>? memory = owner;
            public Memory<byte> Memory => memory!.Memory;
            public void Dispose() { if (Interlocked.Exchange(ref memory, null) is { } value) { value.Dispose(); Interlocked.Decrement(ref pool.Active); } }
        }
    }

}
