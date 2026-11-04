using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using Apache.Fory;
using Google.FlatBuffers;
using ShardPack.Experiment;
using ShardPack.Media;
using SunDoge.ShardPack;
using OpenCvSharp;

// Deliberately use identical logical fields in every format, including MIME/ID.
[ForyStruct]
public sealed class Metadata
{
    public ulong Id { get; set; }
    public uint Width { get; set; }
    public uint Height { get; set; }
    public string Mime { get; set; } = "image/jpeg";
    public float[] Boxes { get; set; } = [];
    public int[] Labels { get; set; } = [];
}
[ForyStruct]
public sealed class Whole
{
    public Metadata Metadata { get; set; } = new();
    public byte[] Image { get; set; } = [];
}
internal sealed record Fixture(Metadata Metadata, byte[] Image);
internal sealed record Measurement(string Operation, string Format, int Round, int Records,
    double Seconds, double RecordsPerSecond, double ManagedBytesPerRecord, int Gen0, int Gen1, int Gen2,
    double Checksum);

internal static class Program
{
    private static readonly string[] Formats = ["flatbuffers", "fory-whole", "fory-metadata-raw"];
    private static Apache.Fory.Fory NewFory()
    {
        var f = Apache.Fory.Fory.Builder().TrackRef(false).Compatible(false).Build();
        f.Register<Metadata>(100); f.Register<Whole>(101); return f;
    }
    private sealed class Codec
    {
        private readonly Apache.Fory.Fory fory = NewFory();
        private readonly FlatBufferBuilder builder = new(1024);
        private readonly ArrayBufferWriter<byte> output = new();
        public byte[] Encode(Fixture f, string format)
        {
            if (format == "fory-whole") return fory.Serialize(new Whole { Metadata = f.Metadata, Image = f.Image });
            if (format == "flatbuffers") return EncodeBorrowed(f, format).ToArray();
            var metadata = fory.Serialize(f.Metadata);
            var result = new byte[4 + metadata.Length + f.Image.Length];
            BinaryPrimitives.WriteUInt32LittleEndian(result, (uint)metadata.Length);
            metadata.CopyTo(result, 4); f.Image.CopyTo(result, 4 + metadata.Length);
            return result;
        }
        // Returned storage is valid only until the next encode on this worker.
        public ReadOnlyMemory<byte> EncodeBorrowed(Fixture f, string format)
        {
            var m = f.Metadata;
            if (format == "flatbuffers")
            {
                builder.Clear();
                var mime = builder.CreateString(m.Mime);
                var data = EncodedImage.CreateDataVectorBlock(builder, f.Image);
                var image = EncodedImage.CreateEncodedImage(builder, mime, m.Width, m.Height, data);
                DetectionSample.StartBoxesVector(builder, m.Labels.Length);
                for (int i = m.Labels.Length - 1; i >= 0; i--)
                    Box.CreateBox(builder, m.Boxes[i * 4], m.Boxes[i * 4 + 1], m.Boxes[i * 4 + 2], m.Boxes[i * 4 + 3], m.Labels[i]);
                var boxes = builder.EndVector();
                DetectionSample.FinishDetectionSampleBuffer(builder,
                    DetectionSample.CreateDetectionSample(builder, m.Id, image, boxes));
                var segment = builder.DataBuffer.ToArraySegment(builder.DataBuffer.Position, builder.Offset);
                return segment.AsMemory();
            }
            output.Clear();
            if (format == "fory-whole")
                fory.Serialize(output, new Whole { Metadata = m, Image = f.Image });
            else
            {
                output.GetSpan(4)[..4].Clear(); output.Advance(4);
                fory.Serialize(output, m);
                int metadataBytes = output.WrittenCount - 4;
                // ArrayBufferWriter exposes a read-only view; build the prefix
                // in a separate reusable buffer is unnecessary: its backing array
                // is owned exclusively by this codec and never escapes concurrently.
                if (!MemoryMarshal.TryGetArray(output.WrittenMemory, out var array)) throw new Exception();
                BinaryPrimitives.WriteUInt32LittleEndian(array.AsSpan(), (uint)metadataBytes);
                output.Write(f.Image);
            }
            return output.WrittenMemory;
        }
        private T Deserialize<T>(ReadOnlyMemory<byte> bytes)
        {
            if (MemoryMarshal.TryGetArray(bytes, out var a) && a.Offset == 0 && a.Count == a.Array!.Length)
                return fory.Deserialize<T>(a.Array);
            return fory.Deserialize<T>(bytes.Span);
        }
        public void Validate(byte[] bytes, string format, Fixture expected)
        {
            Metadata actual; ReadOnlyMemory<byte> blob;
            if (format == "flatbuffers")
            {
                var buffer = BorrowedImage.Buffer(bytes);
                if (!DetectionSample.VerifyDetectionSample(buffer)) throw new Exception("Invalid FlatBuffer");
                var value = DetectionSample.GetRootAsDetectionSample(buffer);
                var image = value.Image!.Value;
                actual = new Metadata
                {
                    Id = value.Id,
                    Width = image.Width,
                    Height = image.Height,
                    Mime = image.Mime,
                    Boxes = new float[value.BoxesLength * 4],
                    Labels = new int[value.BoxesLength]
                };
                for (int i = 0; i < value.BoxesLength; i++)
                {
                    var b = value.Boxes(i)!.Value; actual.Boxes[i * 4] = b.X; actual.Boxes[i * 4 + 1] = b.Y;
                    actual.Boxes[i * 4 + 2] = b.Width; actual.Boxes[i * 4 + 3] = b.Height; actual.Labels[i] = b.Label;
                }
                blob = BorrowedImage.Data(image);
            }
            else if (format == "fory-whole") { var w = Deserialize<Whole>(bytes); actual = w.Metadata; blob = w.Image; }
            else
            { int n = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(bytes)); actual = Deserialize<Metadata>(bytes.AsMemory(4, n)); blob = bytes.AsMemory(4 + n); }
            var m = expected.Metadata;
            if (actual.Id != m.Id || actual.Width != m.Width || actual.Height != m.Height || actual.Mime != m.Mime ||
                !actual.Boxes.AsSpan().SequenceEqual(m.Boxes) || !actual.Labels.AsSpan().SequenceEqual(m.Labels) || !blob.Span.SequenceEqual(expected.Image))
                throw new Exception("Field-by-field round trip mismatch: " + format);
            bool aliases = MemoryMarshal.TryGetArray(blob, out var segment) && ReferenceEquals(segment.Array, bytes);
            if (aliases != (format != "fory-whole")) throw new Exception("Unexpected blob ownership: " + format);
        }
        public (double Sum, ReadOnlyMemory<byte> Image) Read(ReadOnlyMemory<byte> bytes, string format, bool metadataOnly, bool headerOnly = false)
        {
            if (format == "flatbuffers")
            {
                var buffer = BorrowedImage.Buffer(bytes);
                if (!DetectionSample.VerifyDetectionSample(buffer)) throw new System.IO.InvalidDataException("Invalid FlatBuffer");
                var s = DetectionSample.GetRootAsDetectionSample(buffer);
                var image = s.Image!.Value;
                double sum = s.Id + image.Width + image.Height;
                var mime = image.GetMimeBytes()!.Value;
                foreach (byte b in mime.AsSpan()) sum += b;
                for (int i = 0; !headerOnly && i < s.BoxesLength; i++)
                { var b = s.Boxes(i)!.Value; sum += (double)b.X + b.Y + b.Width + b.Height + b.Label; }
                return (sum, metadataOnly ? default : BorrowedImage.Data(image));
            }
            Metadata m; ReadOnlyMemory<byte> blob;
            if (format == "fory-whole") { var s = Deserialize<Whole>(bytes); m = s.Metadata; blob = s.Image; }
            else
            {
                int n = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.Span));
                m = Deserialize<Metadata>(bytes.Slice(4, n)); blob = bytes[(4 + n)..];
            }
            double total = m.Id + m.Width + m.Height;
            foreach (char c in m.Mime) total += c;
            for (int i = 0; !headerOnly && i < m.Labels.Length; i++)
                total += (double)m.Boxes[i * 4] + m.Boxes[i * 4 + 1] + m.Boxes[i * 4 + 2] + m.Boxes[i * 4 + 3] + m.Labels[i];
            return (total, metadataOnly ? default : blob);
        }
    }
    private static Fixture[] Load(string coco, int count)
    {
        using var doc = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(coco, "annotations/instances_val2017.json")));
        var annotations = doc.RootElement.GetProperty("annotations").EnumerateArray()
            .Where(a => a.GetProperty("iscrowd").GetInt32() == 0)
            .GroupBy(a => a.GetProperty("image_id").GetUInt64()).ToDictionary(g => g.Key, g => g.ToArray());
        return doc.RootElement.GetProperty("images").EnumerateArray().OrderBy(i => i.GetProperty("id").GetUInt64())
            .Take(count).Select(i =>
            {
                ulong id = i.GetProperty("id").GetUInt64();
                var boxes = annotations.GetValueOrDefault(id, []);
                return new Fixture(new Metadata
                {
                    Id = id,
                    Width = i.GetProperty("width").GetUInt32(),
                    Height = i.GetProperty("height").GetUInt32(),
                    Boxes = boxes.SelectMany(a => a.GetProperty("bbox").EnumerateArray().Select(v => v.GetSingle())).ToArray(),
                    Labels = boxes.Select(a => a.GetProperty("category_id").GetInt32()).ToArray()
                },
                    File.ReadAllBytes(Path.Combine(coco, "val2017", i.GetProperty("file_name").GetString()!)));
            }).ToArray();
    }
    private static Measurement Measure(string operation, string format, int round, int count, Func<double> action)
    {
        GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
        long allocated = GC.GetAllocatedBytesForCurrentThread();
        int g0 = GC.CollectionCount(0), g1 = GC.CollectionCount(1), g2 = GC.CollectionCount(2);
        long start = Stopwatch.GetTimestamp(); double checksum = action();
        double seconds = Stopwatch.GetElapsedTime(start).TotalSeconds;
        return new(operation, format, round, count, seconds, count / seconds,
            (double)(GC.GetAllocatedBytesForCurrentThread() - allocated) / count,
            GC.CollectionCount(0) - g0, GC.CollectionCount(1) - g1, GC.CollectionCount(2) - g2, checksum);
    }
    public static void Main(string[] args)
    {
        if (args.Length is < 2 or > 3) throw new ArgumentException("Usage: RecordCodecs <COCO2017 directory> <results.json> [synthetic-box-count]");
        var fixtures = Load(args[0], 256);
        Cv2.SetNumThreads(1); // Explicit process-wide policy for this benchmark only.
        using var resized = new Mat();
        double Preprocess(ReadOnlyMemory<byte> jpeg)
        {
            using var decoded = Cv2.ImDecode(jpeg.Span, ImreadModes.Color | ImreadModes.IgnoreOrientation);
            if (decoded.Empty()) throw new Exception("JPEG decode failed");
            Cv2.Resize(decoded, resized, new Size(320, 320), 0, 0, InterpolationFlags.Linear);
            var pixel = resized.At<Vec3b>(0, 0);
            return pixel.Item0 + pixel.Item1 + pixel.Item2;
        }
        if (args.Length == 3)
        {
            int count = int.Parse(args[2]);
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
            foreach (var f in fixtures)
            {
                var m = f.Metadata;
                m.Boxes = Enumerable.Range(0, count * 4).Select(i => m.Boxes.Length == 0 ? 0f : m.Boxes[i % m.Boxes.Length]).ToArray();
                m.Labels = Enumerable.Range(0, count).Select(i => m.Labels.Length == 0 ? 1 : m.Labels[i % m.Labels.Length]).ToArray();
            }
        }
        var codec = new Codec();
        var encoded = Formats.ToDictionary(f => f, f => fixtures.Select(x => codec.Encode(x, f)).ToArray());
        // Full byte equality and metadata checks before timing, not just lengths.
        for (int i = 0; i < fixtures.Length; i++)
        {
            var baseline = codec.Read(encoded[Formats[0]][i], Formats[0], false);
            foreach (string f in Formats)
            {
                if (!codec.EncodeBorrowed(fixtures[i], f).Span.SequenceEqual(encoded[f][i]))
                    throw new Exception("Reused encoding differs: " + f);
                codec.Validate(encoded[f][i], f, fixtures[i]);
                var read = codec.Read(encoded[f][i], f, false);
                if (read.Sum != baseline.Sum || !read.Image.Span.SequenceEqual(fixtures[i].Image)) throw new Exception("Round trip differs: " + f);
            }
        }
        var flat = encoded["flatbuffers"][0];
        var truncated = flat[..(flat.Length / 2)];
        if (DetectionSample.VerifyDetectionSample(BorrowedImage.Buffer(truncated))) throw new Exception("Truncation accepted");
        // A pool bucket can be larger than the record. Padding must not become
        // readable record data, even if the original bytes remain in that bucket.
        if (DetectionSample.VerifyDetectionSample(BorrowedImage.Buffer(flat.AsMemory(0, flat.Length / 2))))
            throw new Exception("Verifier escaped the logical record length");
        if (System.Environment.GetEnvironmentVariable("SHARDPACK_CODEC_VERIFY_ONLY") == "1")
        {
            Console.WriteLine($"Verified {fixtures.Length * Formats.Length} records, blob ownership, and truncated/bounded buffers.");
            return;
        }
        string directory = Path.Combine(Path.GetTempPath(), "shardpack-codec-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        var results = new List<Measurement>();
        try
        {
            foreach (string format in Formats)
                foreach (var compression in new[] { BlockCodec.None, BlockCodec.Zstd })
                {
                    using var writer = new ShardWriter(File.Create(Path.Combine(directory, format + compression + ".spk")), new() { Codec = compression });
                    foreach (var body in encoded[format]) writer.Write(new byte[16], body);
                    writer.Finish();
                }
            // Warm every record/API and JIT before collecting rounds.
            foreach (string format in Formats) foreach (var f in fixtures) codec.EncodeBorrowed(f, format);
            foreach (string format in Formats) foreach (var b in encoded[format]) codec.Read(b, format, false);
            for (int round = 0; round < 7; round++)
                foreach (string format in round % 2 == 0 ? Formats : Formats.Reverse())
                    foreach (string operation in new[] { "encode-owned", "encode-reused", "read-header", "read-metadata", "read-blob", "read-pooled-shard-none", "read-pooled-shard-zstd", "read-preprocess", "read-pooled-shard-preprocess" })
                    {
                        int passes = operation.Contains("preprocess") ? 1 : operation.StartsWith("read-pooled-shard") ? 2 : operation.StartsWith("encode") || format == "fory-whole" ? 8 : 64;
                        double Run()
                        {
                            double sum = 0;
                            for (int pass = 0; pass < passes; pass++)
                            {
                                if (operation.StartsWith("read-pooled-shard"))
                                {
                                    string compression = operation.EndsWith("zstd") ? "Zstd" : "None";
                                    using var reader = new ShardReader(File.OpenRead(Path.Combine(directory, format + compression + ".spk")));
                                    int n = 0;
                                    while (reader.ReadPooled() is { } record)
                                    {
                                        using (record)
                                        {
                                            var v = codec.Read(record.Payload, format, false); sum += v.Sum + v.Image.Span[0] + v.Image.Span[^1];
                                            if (operation.Contains("preprocess")) sum += Preprocess(v.Image); n++;
                                        }
                                    }
                                    if (n != fixtures.Length) throw new Exception("Missing record");
                                }
                                else for (int i = 0; i < fixtures.Length; i++)
                                {
                                    if (operation == "encode-owned") sum += codec.Encode(fixtures[i], format).Length;
                                    else if (operation == "encode-reused") sum += codec.EncodeBorrowed(fixtures[i], format).Length;
                                    else
                                    {
                                        var v = codec.Read(encoded[format][i], format, operation is "read-metadata" or "read-header", operation == "read-header"); sum += v.Sum; if (!v.Image.IsEmpty) sum += v.Image.Span[0] + v.Image.Span[^1];
                                        if (operation.Contains("preprocess")) sum += Preprocess(v.Image);
                                    }
                                }
                            }
                            return sum / passes;
                        }
                        _ = Run();
                        var measurement = Measure(operation, format, round, passes * fixtures.Length, Run);
                        results.Add(measurement); Console.WriteLine(JsonSerializer.Serialize(measurement));
                    }
        }
        finally { Directory.Delete(directory, recursive: true); }
        foreach (var group in results.Where(r => !r.Operation.StartsWith("encode")).GroupBy(r => r.Operation))
        {
            double expected = group.First().Checksum;
            if (group.Any(r => Math.Abs(r.Checksum - expected) > Math.Abs(expected) * 1e-10))
                throw new Exception("Timed reads differ: " + group.Key);
        }
        var report = new
        {
            Environment = new
            {
                Dotnet = System.Environment.Version.ToString(),
                OS = RuntimeInformation.OSDescription,
                CPU = File.ReadLines("/proc/cpuinfo").First(l => l.StartsWith("model name")),
                Fory = "1.7.7",
                FlatBuffers = "25.2.10",
                OpenCV = Cv2.GetVersionString(),
                OpenCVThreads = Cv2.GetNumThreads()
            },
            Corpus = new
            {
                Records = fixtures.Length,
                JpegBytes = fixtures.Sum(f => (long)f.Image.Length),
                Boxes = fixtures.Sum(f => f.Metadata.Labels.Length),
                Sha256 = Convert.ToHexString(SHA256.HashData(fixtures.SelectMany(f => SHA256.HashData(f.Image)).ToArray())),
                PayloadBytes = encoded.ToDictionary(kv => kv.Key, kv => kv.Value.Sum(b => (long)b.Length))
            },
            Results = results
        };
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(args[1]))!);
        File.WriteAllText(args[1], JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));
    }
}
