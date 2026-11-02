using System.Runtime.InteropServices;
using SunDoge.ShardPack.Interop;

namespace SunDoge.ShardPack;

public sealed record ShardRecord(byte[] SampleId, uint Flags, byte[] Payload);

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

    public void Dispose()
    {
        lock (gate)
        {
            if (busy) throw new InvalidOperationException("Cannot dispose during a stream callback.");
            handle.Dispose();
        }
    }
}
