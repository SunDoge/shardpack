using SunDoge.ShardPack;

static void Equal(ReadOnlySpan<byte> actual, ReadOnlySpan<byte> expected)
{
    if (!actual.SequenceEqual(expected)) throw new Exception("Byte comparison failed.");
}
static void Assert(bool condition) { if (!condition) throw new Exception("Assertion failed."); }
static T Throws<T>(Action action) where T : Exception
{
    try { action(); }
    catch (T error) { return error; }
    throw new Exception($"Expected {typeof(T).Name}.");
}

string root = Path.GetFullPath(args.Length == 0 ? "." : args[0]);
string fixtures = Path.Combine(root, "crates/shardpack-format/tests/data");
byte[] Golden(string name) => Convert.FromHexString(string.Concat(File.ReadAllText(Path.Combine(fixtures, name)).Where(c => !char.IsWhiteSpace(c))));
byte[] id = Enumerable.Range(0, 16).Select(i => (byte)i).ToArray();
byte[] secondId = Enumerable.Range(0, 16).Select(i => (byte)(255 - i)).ToArray();

// C# writer must emit the exact Rust plaintext golden bytes, not merely decode itself.
using var plain = new MemoryStream();
using (var writer = new ShardWriter(plain, new() { TargetBlockBytes = 1024 }, leaveOpen: true))
{
    writer.Write(id, "alpha"u8, 0x11223344);
    writer.Write(secondId, [0, 1, 2, 3], 0xaabbccdd);
    var summary = writer.Finish();
    Assert(summary.RecordCount == 2 && summary.BlockCount == 1 && summary.ByteSize == (ulong)plain.Length);
    Assert(Convert.ToHexString(summary.ShardId) == "490504DD4B188F0B601B3FC6100F904CA725DCA43FED11C81987C5ED2C778D3A");
    Throws<InvalidOperationException>(() => writer.Finish());
    Throws<InvalidOperationException>(() => writer.Write(id, []));
}
Equal(plain.ToArray(), Golden("v1_none.hex"));
using (var reader = new ShardReader(new ChunkedStream(plain.ToArray())))
{
    var first = reader.Read()!;
    Equal(first.SampleId, id);
    Equal(first.Payload, "alpha"u8);
    Assert(first.Flags == 0x11223344);
    var second = reader.Read()!;
    Equal(second.SampleId, secondId);
    Equal(second.Payload, [0, 1, 2, 3]);
    Assert(reader.Read() == null && reader.Read() == null);
    Equal(first.Payload, "alpha"u8); // previous record survives further reads
}

byte[] key = Enumerable.Repeat((byte)0x11, 32).ToArray();
using (var reader = new ShardReader(new ChunkedStream(Golden("v1_aes256gcm.hex")), key))
{
    var record = reader.Read()!;
    Equal(record.SampleId, Enumerable.Repeat((byte)0x33, 16).ToArray());
    Equal(record.Payload, "alpha"u8);
    Assert(record.Flags == 0x12345678 && reader.Read() == null);
}

foreach (var codec in new[] { BlockCodec.None, BlockCodec.Zstd })
foreach (bool encrypted in new[] { false, true })
{
    byte[] encryptionKey = encrypted ? key : [];
    using var output = new MemoryStream();
    // A non-seekable write stream proves neither Length nor Position is needed.
    using (var writer = new ShardWriter(new WriteOnlyStream(output), new() { TargetBlockBytes = 40, Codec = codec }, encryptionKey))
    {
        writer.Write(id, []);
        writer.Write(secondId, new byte[1024]); // jumbo block
        Assert(writer.Finish().RecordCount == 2);
    }
    byte[] bytes = output.ToArray();
    using (var reader = new ShardReader(new ChunkedStream(bytes), encryptionKey))
    {
        Assert(reader.Read()!.Payload.Length == 0);
        Assert(reader.Read()!.Payload.Length == 1024);
        Assert(reader.Read() == null);
    }
    var corrupt = bytes.ToArray(); corrupt[^1] ^= 1;
    using (var reader = new ShardReader(new ChunkedStream(corrupt), encryptionKey))
        Throws<ShardPackException>(() => { while (reader.Read() != null) { } });
    using (var reader = new ShardReader(new ChunkedStream(bytes[..^1]), encryptionKey))
        Throws<ShardPackException>(() => { while (reader.Read() != null) { } });
    if (encrypted)
    {
        using var wrong = new ShardReader(new ChunkedStream(bytes), new byte[32]);
        Throws<ShardPackException>(() => wrong.Read());
        Throws<ShardPackException>(() => new ShardReader(new ChunkedStream(bytes)));
    }
}

using (var reader = new ShardReader(new ChunkedStream(Golden("v1_none.hex")),
    limits: new() { MaxRecordsPerBlock = 1 }))
    Throws<ShardPackException>(() => reader.Read());
Throws<ArgumentException>(() => new ShardWriter(new MemoryStream(), key: new byte[31]));
Throws<ArgumentException>(() => new ShardReader(new MemoryStream(), key: new byte[1]));
using (var writer = new ShardWriter(new MemoryStream()))
    Throws<ArgumentException>(() => writer.Write(new byte[15], []));
using (var failing = new FailureStream())
using (var writer = new ShardWriter(failing, new() { TargetBlockBytes = 1 }, leaveOpen: true))
{
    failing.FailWrite = true;
    var error = Throws<ShardPackException>(() => writer.Write(id, "test"u8));
    Assert(error.InnerException is IOException);
    Throws<ShardPackException>(() => writer.Write(id, "retry"u8));
    Throws<ShardPackException>(() => writer.Finish());
}
using (var failing = new FailureStream())
using (var writer = new ShardWriter(failing, leaveOpen: true))
{
    failing.FailFlush = true;
    Throws<ShardPackException>(() => writer.Finish());
}
var readFailure = Throws<ShardPackException>(() => new ShardReader(new FailureStream()));
Assert(readFailure.InnerException is IOException);
using (var stream = new MemoryStream())
{
    using (var writer = new ShardWriter(stream, leaveOpen: true)) { }
    Assert(stream.CanWrite && stream.Length == 64); // no implicit finish
}
var owned = new MemoryStream();
var disposable = new ShardWriter(owned);
disposable.Dispose(); disposable.Dispose();
Assert(!owned.CanWrite);
Throws<ObjectDisposedException>(() => disposable.Finish());

using (var stream = new FailureStream())
using (var writer = new ShardWriter(stream, new() { TargetBlockBytes = 1 }, leaveOpen: true))
{
    stream.OnWrite = writer.Dispose;
    var error = Throws<ShardPackException>(() => writer.Write(id, "reentrant"u8));
    Assert(error.InnerException is InvalidOperationException);
}
Console.WriteLine("All ShardPack .NET interoperability tests passed.");

sealed class ChunkedStream(byte[] bytes) : Stream
{
    private readonly MemoryStream inner = new(bytes);
    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override int Read(Span<byte> buffer) => inner.Read(buffer[..Math.Min(buffer.Length, 3)]);
    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));
    public override void Flush() => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long length) => throw new NotSupportedException();
    protected override void Dispose(bool disposing) { if (disposing) inner.Dispose(); base.Dispose(disposing); }
}
sealed class WriteOnlyStream(MemoryStream output) : Stream
{
    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() => output.Flush();
    public override void Write(ReadOnlySpan<byte> buffer) => output.Write(buffer);
    public override void Write(byte[] buffer, int offset, int count) => output.Write(buffer, offset, count);
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long length) => throw new NotSupportedException();
}
sealed class FailureStream : MemoryStream
{
    public bool FailWrite;
    public bool FailFlush;
    public Action? OnWrite;
    public override int Read(Span<byte> buffer) => throw new IOException("injected read failure");
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        OnWrite?.Invoke();
        if (FailWrite) throw new IOException("injected write failure");
        base.Write(buffer);
    }
    public override void Flush()
    {
        if (FailFlush) throw new IOException("injected flush failure");
        base.Flush();
    }
}
