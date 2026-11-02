using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using SunDoge.ShardPack.Interop;

namespace SunDoge.ShardPack;

internal sealed unsafe class StreamBridge
{
    internal readonly Stream Stream;
    private readonly bool leaveOpen;
    internal Exception? Failure;
    internal StreamBridge(Stream stream, bool leaveOpen)
    {
        Stream = stream;
        this.leaveOpen = leaveOpen;
    }

    internal StreamCallbacks Callbacks(GCHandle context) => new()
    {
        context = (void*)GCHandle.ToIntPtr(context), read = &Read, write = &Write, flush = &Flush
    };

    internal static void Check(int status, StreamBridge bridge)
    {
        if (status >= 0) return;
        string message = Marshal.PtrToStringUTF8((nint)NativeMethods.spk_last_error()) ?? "Native ShardPack error";
        throw new ShardPackException(message, bridge.Failure);
    }

    internal static void ValidateKey(ReadOnlySpan<byte> key)
    {
        if (!key.IsEmpty && key.Length != 32)
            throw new ArgumentException("An AES-256 key must contain exactly 32 bytes.", nameof(key));
        if (NativeMethods.spk_abi_version() != 1)
            throw new NotSupportedException("Unsupported ShardPack native ABI.");
    }

    internal void Close()
    {
        if (!leaveOpen) Stream.Dispose();
    }

    private static StreamBridge Get(void* context) => (StreamBridge)GCHandle.FromIntPtr((nint)context).Target!;

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static nint Read(void* context, byte* buffer, nuint length)
    {
        var state = Get(context);
        try { return state.Stream.Read(new Span<byte>(buffer, (int)Math.Min(length, (nuint)int.MaxValue))); }
        catch (Exception error) { state.Failure = error; return -1; }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static nint Write(void* context, byte* buffer, nuint length)
    {
        var state = Get(context);
        try
        {
            int count = (int)Math.Min(length, (nuint)int.MaxValue);
            state.Stream.Write(new ReadOnlySpan<byte>(buffer, count));
            return count;
        }
        catch (Exception error) { state.Failure = error; return -1; }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static int Flush(void* context)
    {
        var state = Get(context);
        try { state.Stream.Flush(); return 0; }
        catch (Exception error) { state.Failure = error; return -1; }
    }
}

internal sealed unsafe class NativeHandle : SafeHandle
{
    private GCHandle context;
    private readonly bool writer;
    internal NativeHandle(nint pointer, GCHandle context, bool writer) : base(0, true)
    {
        this.context = context;
        this.writer = writer;
        SetHandle(pointer);
    }
    public override bool IsInvalid => handle == 0;
    protected override bool ReleaseHandle()
    {
        if (writer) NativeMethods.spk_writer_close((WriterHandle*)handle);
        else NativeMethods.spk_reader_close((ReaderHandle*)handle);
        if (context.IsAllocated)
        {
            // SafeHandle finalization must never propagate managed stream errors.
            try { ((StreamBridge)context.Target!).Close(); }
            catch { }
            finally { context.Free(); }
        }
        return true;
    }
}

public sealed class ShardPackException : IOException
{
    internal ShardPackException(string message, Exception? inner = null) : base(message, inner) { }
}
