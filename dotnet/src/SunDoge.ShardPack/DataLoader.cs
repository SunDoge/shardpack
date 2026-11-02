using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;

namespace SunDoge.ShardPack;

public sealed record DataLoaderOptions
{
    public int BatchSize { get; init; } = 1;
    public int DecodeWorkers { get; init; } = 2;
    /// <summary>Total queued/decoding/reordered records, excluding the shuffle pool and assembling batch.</summary>
    public int PrefetchRecords { get; init; } = 16;
    public int PrefetchBatches { get; init; } = 2;
    public int ShuffleCapacity { get; init; } = 1;
    public int Seed { get; init; }
    public bool DropLast { get; init; } = true;
    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(BatchSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(DecodeWorkers);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(PrefetchRecords);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(PrefetchBatches);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ShuffleCapacity);
    }
}

public static class ShardSource
{
    /// <summary>Own each sequential reader until EOF or enumeration disposal. No filesystem scan or hidden retry.</summary>
    public static IEnumerable<IShardRecord> ReadFiles(IEnumerable<string> paths, ReaderLimits? limits = null)
    {
        ArgumentNullException.ThrowIfNull(paths);
        foreach (var path in paths)
        {
            using var reader = new ShardReader(File.OpenRead(path), limits: limits);
            while (reader.Read() is { } record) yield return record;
        }
    }
    public static IEnumerable<IShardRecord> ReadPooledFiles(IEnumerable<string> paths, ReaderLimits? limits = null)
    {
        ArgumentNullException.ThrowIfNull(paths);
        foreach (var path in paths)
        {
            using var reader = new ShardReader(File.OpenRead(path), limits: limits);
            while (reader.ReadPooled() is { } record) yield return record;
        }
    }

}

/// <summary>Bounded background reading, deterministic shuffle, parallel decode and ordered collation.
/// Decode/collate run on a fixed number of workers. Collate borrows its samples;
/// disposable samples are released afterward. Consumers own yielded disposable batches.</summary>
public sealed class DataLoader<TSample, TBatch>
{
    private readonly Func<CancellationToken, IEnumerable<IShardRecord>> source;
    private readonly Func<IShardRecord, CancellationToken, TSample> decode;
    private readonly Func<IReadOnlyList<TSample>, CancellationToken, TBatch> collate;
    private readonly DataLoaderOptions options;
    public DataLoader(Func<CancellationToken, IEnumerable<IShardRecord>> source,
        Func<IShardRecord, CancellationToken, TSample> decode,
        Func<IReadOnlyList<TSample>, CancellationToken, TBatch> collate,
        DataLoaderOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(source); ArgumentNullException.ThrowIfNull(decode); ArgumentNullException.ThrowIfNull(collate);
        this.source = source; this.decode = decode; this.collate = collate;
        this.options = options ?? new(); this.options.Validate();
    }
    private static void Release<T>(T value) { if (value is IDisposable disposable) disposable.Dispose(); }
    private static Channel<T> Queue<T>(int capacity, bool singleReader, bool singleWriter) => Channel.CreateBounded<T>(new BoundedChannelOptions(capacity) {
        SingleReader = singleReader, SingleWriter = singleWriter, FullMode = BoundedChannelFullMode.Wait, AllowSynchronousContinuations = false
    });

    public async IAsyncEnumerable<TBatch> ReadEpochAsync(int epoch = 0, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var token = stopping.Token;
        using var slots = new SemaphoreSlim(options.PrefetchRecords, options.PrefetchRecords);
        var jobs = Queue<(long Index, IShardRecord Record)>(options.PrefetchRecords, false, true);
        var decoded = Queue<(long Index, TSample Sample)>(options.PrefetchRecords, true, false);
        var batches = Queue<TBatch>(options.PrefetchBatches, true, true);
        ExceptionDispatchInfo? failure = null;
        void Fail(Exception error)
        {
            if (error is OperationCanceledException && token.IsCancellationRequested) return;
            Interlocked.CompareExchange(ref failure, ExceptionDispatchInfo.Capture(error), null);
            stopping.Cancel();
        }
        var producer = Task.Run(async () => {
            var pool = new List<IShardRecord>(options.ShuffleCapacity);
            try
            {
                var rng = new Random(unchecked(options.Seed + epoch));
                long index = 0;
                async ValueTask Enqueue(IShardRecord record)
                {
                    await slots.WaitAsync(token).ConfigureAwait(false);
                    await jobs.Writer.WriteAsync((index++, record), token).ConfigureAwait(false);
                }
                foreach (var record in source(token))
                {
                    bool retained = false;
                    try
                    {
                        token.ThrowIfCancellationRequested();
                        if (pool.Count < options.ShuffleCapacity) { pool.Add(record); retained = true; continue; }
                        int selected = rng.Next(pool.Count);
                        var next = pool[selected]; pool[selected] = record; retained = true;
                        try { await Enqueue(next).ConfigureAwait(false); } catch { Release(next); throw; }
                    }
                    finally { if (!retained) Release(record); }
                }
                while (pool.Count > 0)
                {
                    int selected = rng.Next(pool.Count);
                    var next = pool[selected]; pool.RemoveAt(selected);
                    try { await Enqueue(next).ConfigureAwait(false); } catch { Release(next); throw; }
                }
            }
            catch (Exception error) { Fail(error); }
            finally { foreach (var record in pool) Release(record); jobs.Writer.TryComplete(); }
        });
        var workers = Enumerable.Range(0, options.DecodeWorkers).Select(_ => Task.Run(async () => {
            try
            {
                await foreach (var job in jobs.Reader.ReadAllAsync(token).ConfigureAwait(false))
                {
                    TSample sample;
                    try { token.ThrowIfCancellationRequested(); sample = decode(job.Record, token); }
                    finally { Release(job.Record); }
                    try { await decoded.Writer.WriteAsync((job.Index, sample), token).ConfigureAwait(false); }
                    catch { Release(sample); throw; }
                }
            }
            catch (Exception error) { Fail(error); }
        })).ToArray();
        async Task FinishDecode()
        {
            await Task.WhenAll(workers).ConfigureAwait(false);
            decoded.Writer.TryComplete();
        }
        var decoding = FinishDecode();
        var batching = Task.Run(async () => {
            var pending = new SortedDictionary<long, TSample>();
            var samples = new List<TSample>(options.BatchSize);
            long next = 0;
            async ValueTask Emit()
            {
                TBatch batch;
                try { batch = collate(samples, token); }
                finally { foreach (var sample in samples) Release(sample); samples.Clear(); }
                try { await batches.Writer.WriteAsync(batch, token).ConfigureAwait(false); }
                catch { Release(batch); throw; }
            }
            try
            {
                await foreach (var item in decoded.Reader.ReadAllAsync(token).ConfigureAwait(false))
                {
                    pending.Add(item.Index, item.Sample);
                    while (pending.ContainsKey(next))
                    {
                        token.ThrowIfCancellationRequested();
                        var sample = pending[next]; pending.Remove(next++);
                        samples.Add(sample); slots.Release();
                        if (samples.Count == options.BatchSize) await Emit().ConfigureAwait(false);
                    }
                }
                if (samples.Count != 0 && !options.DropLast) await Emit().ConfigureAwait(false);
            }
            catch (Exception error) { Fail(error); }
            finally
            {
                foreach (var sample in samples) Release(sample);
                foreach (var sample in pending.Values) Release(sample);
                batches.Writer.TryComplete(failure?.SourceException);
            }
        });
        try
        {
            await foreach (var batch in batches.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false)) yield return batch;
        }
        finally
        {
            stopping.Cancel();
            await Task.WhenAll(producer, decoding, batching).ConfigureAwait(false);
            while (jobs.Reader.TryRead(out var job)) Release(job.Record);
            while (decoded.Reader.TryRead(out var sample)) Release(sample.Sample);
            while (batches.Reader.TryRead(out var batch)) Release(batch);
        }
    }
}
