using System.Buffers.Binary;
using System.Runtime.InteropServices;
using SunDoge.ShardPack.Interop;

namespace SunDoge.ShardPack;

public enum BlockCodec : byte { None = 0, Zstd = 1 }
public sealed record WriterOptions
{
    public int TargetBlockBytes { get; init; } = 16 * 1024 * 1024;
    public BlockCodec Codec { get; init; } = BlockCodec.None;
    public int ZstdLevel { get; init; } = 1;
}
public sealed record ShardSummary(byte[] ShardId, ulong ByteSize, ulong RecordCount,
    uint BlockCount, byte[] AccumulatorA, byte[] AccumulatorB);

/// <summary>Call Finish explicitly to flush the final block; Dispose aborts unfinished writes.</summary>
public sealed unsafe class ShardWriter : IDisposable
{
    private readonly object gate = new();
    private readonly NativeHandle handle;
    private readonly StreamBridge bridge;
    private bool finished;
    private bool busy;

    public ShardWriter(Stream stream, WriterOptions? options = null,
        ReadOnlySpan<byte> key = default, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanWrite) throw new ArgumentException("Stream must be writable.", nameof(stream));
        options ??= new();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.TargetBlockBytes);
        if (!Enum.IsDefined(options.Codec)) throw new ArgumentOutOfRangeException(nameof(options));
        StreamBridge.ValidateKey(key);
        bridge = new(stream, leaveOpen);
        var context = GCHandle.Alloc(bridge);
        WriterHandle* pointer = null;
        try
        {
            fixed (byte* keyBytes = key)
                StreamBridge.Check(NativeMethods.spk_writer_open(bridge.Callbacks(context), keyBytes,
                    (nuint)options.TargetBlockBytes, (byte)options.Codec, options.ZstdLevel, &pointer), bridge);
            handle = new((nint)pointer, context, true);
        }
        catch { context.Free(); bridge.Close(); throw; }
    }

    public void Write(ReadOnlySpan<byte> sampleId, ReadOnlySpan<byte> payload, uint flags = 0)
    {
        if (sampleId.Length != 16) throw new ArgumentException("Sample ID must contain exactly 16 bytes.", nameof(sampleId));
        lock (gate)
        {
            EnsureWritable();
            busy = true;
            bool referenced = false;
            try
            {
                handle.DangerousAddRef(ref referenced);
                fixed (byte* id = sampleId)
                fixed (byte* data = payload)
                    StreamBridge.Check(NativeMethods.spk_writer_add((WriterHandle*)handle.DangerousGetHandle(),
                        id, flags, data, (nuint)payload.Length), bridge);
            }
            finally { if (referenced) handle.DangerousRelease(); busy = false; }
        }
    }

    public ShardSummary Finish()
    {
        lock (gate)
        {
            EnsureWritable();
            finished = true;
            busy = true;
            bool referenced = false;
            try
            {
                handle.DangerousAddRef(ref referenced);
                Span<byte> summary = stackalloc byte[116];
                fixed (byte* bytes = summary)
                    StreamBridge.Check(NativeMethods.spk_writer_finish((WriterHandle*)handle.DangerousGetHandle(), bytes), bridge);
                return new(summary[..32].ToArray(), BinaryPrimitives.ReadUInt64LittleEndian(summary[32..]),
                    BinaryPrimitives.ReadUInt64LittleEndian(summary[40..]), BinaryPrimitives.ReadUInt32LittleEndian(summary[48..]),
                    summary[52..84].ToArray(), summary[84..116].ToArray());
            }
            finally { if (referenced) handle.DangerousRelease(); busy = false; }
        }
    }

    private void EnsureWritable()
    {
        ObjectDisposedException.ThrowIf(handle.IsClosed, this);
        if (busy) throw new InvalidOperationException("Reentrant writer operations are not supported.");
        if (finished) throw new InvalidOperationException("Writer already finished.");
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
