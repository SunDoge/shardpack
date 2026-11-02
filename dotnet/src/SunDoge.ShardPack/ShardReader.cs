using System.Runtime.InteropServices;
using System.Buffers;
using SunDoge.ShardPack.Interop;

namespace SunDoge.ShardPack;

public interface IShardRecord
{
    ReadOnlyMemory<byte> SampleId { get; }
    uint Flags { get; }
    ReadOnlyMemory<byte> Payload { get; }
}

public sealed record ShardRecord(byte[] SampleId, uint Flags, byte[] Payload) : IShardRecord
{
    ReadOnlyMemory<byte> IShardRecord.SampleId => SampleId;
    ReadOnlyMemory<byte> IShardRecord.Payload => Payload;
}

/// <summary>A pooled copy of one native record. Dispose after decoding; borrowed memory cannot outlive this lease.</summary>
public sealed class PooledShardRecord : IShardRecord, IDisposable
{
    private IMemoryOwner<byte>? owner;
    private readonly int length;
    public ReadOnlyMemory<byte> SampleId { get; }
    public uint Flags { get; }
    public ReadOnlyMemory<byte> Payload => (owner ?? throw new ObjectDisposedException(nameof(PooledShardRecord))).Memory[..length];
    internal PooledShardRecord(byte[] id, uint flags, IMemoryOwner<byte> owner, int length)
    { SampleId = id; Flags = flags; this.owner = owner; this.length = length; }
    public void Dispose() => Interlocked.Exchange(ref owner, null)?.Dispose();
}

public sealed record ReaderLimits
{
    public uint MaxRecordsPerBlock { get; init; } = 1_000_000;
    public ulong MaxStoredBodyBytes { get; init; } = 8UL * 1024 * 1024 * 1024;
    public ulong MaxRawPayloadBytes { get; init; } = 8UL * 1024 * 1024 * 1024;
}

/// <summary>A single sequential cursor. Each returned record owns its buffers.</summary>
public sealed unsafe class ShardReader : IDisposable
{
    private readonly object gate = new();
    private readonly NativeHandle handle;
    private readonly StreamBridge bridge;
    private bool busy;

    public ShardReader(Stream stream, ReadOnlySpan<byte> key = default,
        ReaderLimits? limits = null, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanRead) throw new ArgumentException("Stream must be readable.", nameof(stream));
        StreamBridge.ValidateKey(key);
        limits ??= new();
        bridge = new(stream, leaveOpen);
        var context = GCHandle.Alloc(bridge);
        ReaderHandle* pointer = null;
        try
        {
            fixed (byte* keyBytes = key)
                StreamBridge.Check(NativeMethods.spk_reader_open(bridge.Callbacks(context), keyBytes,
                    limits.MaxRecordsPerBlock, limits.MaxStoredBodyBytes, limits.MaxRawPayloadBytes, &pointer), bridge);
            handle = new((nint)pointer, context, false);
        }
        catch { context.Free(); bridge.Close(); throw; }
    }

    /// <summary>Returns null only at EOF. The input needs neither Length nor Seek.</summary>
    public ShardRecord? Read()
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(handle.IsClosed, this);
            if (busy) throw new InvalidOperationException("Reentrant reader operations are not supported.");
            busy = true;
            bool referenced = false;
            try
            {
                handle.DangerousAddRef(ref referenced);
                byte[] id = new byte[16];
                byte* payload;
                nuint length;
                uint flags;
                int status;
                fixed (byte* idBytes = id)
                    status = NativeMethods.spk_reader_next((ReaderHandle*)handle.DangerousGetHandle(),
                        idBytes, &flags, &payload, &length);
                StreamBridge.Check(status, bridge);
                if (status == 1) return null;
                if (length > int.MaxValue)
                    throw new ShardPackException("Payload exceeds .NET byte[] capacity.");
                return new(id, flags, new ReadOnlySpan<byte>(payload, (int)length).ToArray());
            }
            finally { if (referenced) handle.DangerousRelease(); busy = false; }
        }
    }

    /// <summary>Read into reusable managed memory. The native payload is copied exactly once, while the reader is locked.</summary>
    public PooledShardRecord? ReadPooled(MemoryPool<byte>? pool = null)
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(handle.IsClosed, this);
            if (busy) throw new InvalidOperationException("Reentrant reader operations are not supported.");
            busy = true;
            bool referenced = false;
            try
            {
                handle.DangerousAddRef(ref referenced);
                byte[] id = new byte[16]; byte* payload; nuint length; uint flags;
                int status;
                fixed (byte* idBytes = id)
                    status = NativeMethods.spk_reader_next((ReaderHandle*)handle.DangerousGetHandle(), idBytes, &flags, &payload, &length);
                StreamBridge.Check(status, bridge);
                if (status == 1) return null;
                if (length > int.MaxValue) throw new ShardPackException("Payload exceeds .NET memory capacity.");
                var owner = (pool ?? MemoryPool<byte>.Shared).Rent(Math.Max(1, (int)length));
                try
                {
                    new ReadOnlySpan<byte>(payload, (int)length).CopyTo(owner.Memory.Span);
                    return new(id, flags, owner, (int)length);
                }
                catch { owner.Dispose(); throw; }
            }
            finally { if (referenced) handle.DangerousRelease(); busy = false; }
        }
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (busy) throw new InvalidOperationException("Cannot dispose during a stream callback.");
            handle.Dispose();
        }
    }
}
