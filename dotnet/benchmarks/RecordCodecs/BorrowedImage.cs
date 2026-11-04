using System.Runtime.InteropServices;
using Google.FlatBuffers;

namespace ShardPack.Media;

/// <summary>Helpers over caller-owned bytes. Retain the source buffer/pool lease
/// through the last use of any returned view or memory.</summary>
public static class BorrowedImage
{
    /// <summary>Bound the stock NuGet runtime to the logical length of an array-backed
    /// record. That runtime uses byte arrays; nonzero-offset/non-array memory needs
    /// an explicit copy or a separately built Span-enabled runtime.</summary>
    public static ByteBuffer Buffer(ReadOnlyMemory<byte> bytes)
    {
        if (!MemoryMarshal.TryGetArray(bytes, out ArraySegment<byte> segment) || segment.Array == null || segment.Offset != 0)
            throw new NotSupportedException("Stock FlatBuffers NuGet requires an array-backed record starting at offset zero.");
        return new(new BoundedAllocator(segment.Array, segment.Count), 0);
    }
    /// <summary>Borrow encoded data; generated GetDataArray() would allocate a copy.</summary>
    public static ReadOnlyMemory<byte> Data(EncodedImage image)
    {
        int limit = image.ByteBuffer.Length;
        var mime = image.GetMimeBytes() ?? throw new InvalidDataException("Missing MIME type.");
        var data = image.GetDataBytes() ?? throw new InvalidDataException("Missing image vector.");
        if (mime.Offset < 0 || mime.Offset > limit - mime.Count || data.Offset < 0 || data.Offset > limit - data.Count ||
            image.Width == 0 || image.Height == 0 || data.Count == 0 || !mime.AsSpan().StartsWith("image/"u8))
            throw new InvalidDataException("Invalid encoded-image metadata or buffer bounds.");
        return data.AsMemory();
    }
    private sealed class BoundedAllocator : ByteBufferAllocator
    {
        public BoundedAllocator(byte[] array, int length) { Buffer = array; Length = length; }
        public override void GrowFront(int newSize) => throw new NotSupportedException("Borrowed buffers cannot grow.");
    }
}
