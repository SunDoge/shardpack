using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace SunDoge.ShardPack;

public static class AsyncPrefetch
{
    /// <summary>Prepare at most capacity items ahead, including the in-flight MoveNext.
    /// Ownership of disposable source items transfers here, then to the consumer on yield.
    /// Abandoned queued items are disposed when the consumer breaks/cancels.</summary>
    public static async IAsyncEnumerable<T> Prefetch<T>(this IAsyncEnumerable<T> source, int capacity,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));
        cancellationToken.ThrowIfCancellationRequested();
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var slots = new SemaphoreSlim(capacity, capacity);
        var queue = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity) {
            SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = false, FullMode = BoundedChannelFullMode.Wait
        });
        var producer = Task.Run(async () => {
            Exception? failure = null;
            try
            {
                await using var iterator = source.GetAsyncEnumerator(stopping.Token);
                while (true)
                {
                    await slots.WaitAsync(stopping.Token).ConfigureAwait(false);
                    if (!await iterator.MoveNextAsync().ConfigureAwait(false)) break;
                    var item = iterator.Current;
                    try { await queue.Writer.WriteAsync(item, stopping.Token).ConfigureAwait(false); }
                    catch { if (item is IDisposable disposable) disposable.Dispose(); throw; }
                }
            }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested) { }
            catch (Exception error) { failure = error; }
            finally { queue.Writer.TryComplete(failure); }
        });
        try
        {
            await foreach (var item in queue.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            { slots.Release(); yield return item; }
        }
        finally
        {
            stopping.Cancel(); await producer.ConfigureAwait(false);
            while (queue.Reader.TryRead(out var item)) if (item is IDisposable disposable) disposable.Dispose();
        }
    }
}
