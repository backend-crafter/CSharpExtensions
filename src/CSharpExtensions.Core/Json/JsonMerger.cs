using System.Buffers;
using System.Text.Json;
using CSharpExtensions.Core.Json.Enums;

namespace CSharpExtensions.Core.Json;

/// <summary>
/// Core engine for bounded, document-based structural JSON merge.
/// </summary>
public static class JsonMerger
{
    private const int MaximumDepth = 64;
    private const int MaximumCollectionItems = 100_000;
    private const int MaximumInputBytes = 16 * 1024 * 1024;
    private const int MaximumOutputBytes = 32 * 1024 * 1024;

    /// <summary>
    /// Merges two JSON elements and writes the result to a <see cref="Utf8JsonWriter"/>.
    /// </summary>
    /// <param name="writer">The writer to receive the merged JSON.</param>
    /// <param name="target">The base JSON element.</param>
    /// <param name="source">The JSON element to merge into the target.</param>
    /// <param name="arrayHandling">Strategy for merging arrays.</param>
    public static void Merge(Utf8JsonWriter writer, JsonElement target, JsonElement source, JsonMergeHandling arrayHandling = JsonMergeHandling.Replace)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ValidateArrayHandling(arrayHandling);
        ValidateStructure(target, depth: 0);
        ValidateStructure(source, depth: 0);
        Merge(writer, target, source, arrayHandling, depth: 0);
    }

    private static void Merge(
        Utf8JsonWriter writer,
        JsonElement target,
        JsonElement source,
        JsonMergeHandling arrayHandling,
        int depth)
    {
        if (depth > MaximumDepth)
        {
            throw new JsonException("JSON merge depth limit exceeded.");
        }

        if (target.ValueKind != source.ValueKind)
        {
            source.WriteTo(writer);
            return;
        }

        switch (target.ValueKind)
        {
            case JsonValueKind.Object:
                MergeObjects(writer, target, source, arrayHandling, depth + 1);
                break;
            case JsonValueKind.Array:
                MergeArrays(writer, target, source, arrayHandling, depth + 1);
                break;
            default:
                source.WriteTo(writer);
                break;
        }
    }

    private static void MergeObjects(
        Utf8JsonWriter writer,
        JsonElement target,
        JsonElement source,
        JsonMergeHandling arrayHandling,
        int depth)
    {
        writer.WriteStartObject();

        var sourceProperties = ReadUniqueProperties(source);
        var targetNames = new HashSet<string>(StringComparer.Ordinal);

        foreach (var targetProp in target.EnumerateObject())
        {
            if (targetNames.Count == MaximumCollectionItems || !targetNames.Add(targetProp.Name))
            {
                throw new JsonException("Duplicate JSON property names are not supported during merge.");
            }

            if (!sourceProperties.TryGetValue(targetProp.Name, out var sourceProp))
            {
                targetProp.WriteTo(writer);
            }
            else
            {
                // Property exists in both, merge them
                writer.WritePropertyName(targetProp.Name);
                Merge(writer, targetProp.Value, sourceProp, arrayHandling, depth);
            }
        }

        foreach (var sourceProp in sourceProperties)
        {
            if (!targetNames.Contains(sourceProp.Key))
            {
                writer.WritePropertyName(sourceProp.Key);
                sourceProp.Value.WriteTo(writer);
            }
        }

        writer.WriteEndObject();
    }

    private static void MergeArrays(
        Utf8JsonWriter writer,
        JsonElement target,
        JsonElement source,
        JsonMergeHandling arrayHandling,
        int depth)
    {
        var targetCount = target.GetArrayLength();
        var sourceCount = source.GetArrayLength();
        if (targetCount > MaximumCollectionItems || sourceCount > MaximumCollectionItems)
        {
            throw new JsonException("JSON array item limit exceeded.");
        }

        if (arrayHandling is JsonMergeHandling.Concat or JsonMergeHandling.Union &&
            (long)targetCount + sourceCount > MaximumCollectionItems)
        {
            throw new JsonException("Merged JSON array item limit exceeded.");
        }

        switch (arrayHandling)
        {
            case JsonMergeHandling.Replace:
                source.WriteTo(writer);
                break;

            case JsonMergeHandling.Concat:
                writer.WriteStartArray();
                foreach (var item in target.EnumerateArray()) item.WriteTo(writer);
                foreach (var item in source.EnumerateArray()) item.WriteTo(writer);
                writer.WriteEndArray();
                break;

            case JsonMergeHandling.Union:
                writer.WriteStartArray();
                var written = new HashSet<string>(StringComparer.Ordinal);
                foreach (var item in target.EnumerateArray())
                {
                    var raw = GetCanonicalKey(item);
                    if (written.Add(raw))
                    {
                        item.WriteTo(writer);
                    }
                }
                foreach (var item in source.EnumerateArray())
                {
                    var raw = GetCanonicalKey(item);
                    if (written.Add(raw))
                    {
                        item.WriteTo(writer);
                    }
                }
                writer.WriteEndArray();
                break;

            case JsonMergeHandling.Merge:
                writer.WriteStartArray();
                var targetElements = target.EnumerateArray().ToList();
                var sourceElements = source.EnumerateArray().ToList();
                var maxCount = Math.Max(targetElements.Count, sourceElements.Count);

                for (int i = 0; i < maxCount; i++)
                {
                    if (i < targetElements.Count && i < sourceElements.Count)
                    {
                        Merge(writer, targetElements[i], sourceElements[i], arrayHandling, depth);
                    }
                    else if (i < targetElements.Count)
                    {
                        targetElements[i].WriteTo(writer);
                    }
                    else
                    {
                        sourceElements[i].WriteTo(writer);
                    }
                }
                writer.WriteEndArray();
                break;

            default:
                throw new ArgumentOutOfRangeException(
                    nameof(arrayHandling),
                    arrayHandling,
                    "Unsupported JSON array merge strategy.");
        }
    }

    /// <summary>
    /// High-level merge for byte arrays.
    /// </summary>
    public static byte[] Merge(ReadOnlySpan<byte> targetJson, ReadOnlySpan<byte> sourceJson, JsonMergeHandling arrayHandling = JsonMergeHandling.Replace)
    {
        ValidateArrayHandling(arrayHandling);
        if (targetJson.Length > MaximumInputBytes || sourceJson.Length > MaximumInputBytes)
        {
            throw new JsonException("JSON merge input limit exceeded.");
        }

        var documentOptions = new JsonDocumentOptions { MaxDepth = MaximumDepth };
        using var targetDoc = JsonDocument.Parse(targetJson.ToArray(), documentOptions);
        using var sourceDoc = JsonDocument.Parse(sourceJson.ToArray(), documentOptions);

        var bufferWriter = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(bufferWriter))
        {
            Merge(writer, targetDoc.RootElement, sourceDoc.RootElement, arrayHandling);
        }

        if (bufferWriter.WrittenCount > MaximumOutputBytes)
        {
            throw new JsonException("JSON merge output limit exceeded.");
        }

        return bufferWriter.WrittenSpan.ToArray();
    }

    internal static void ValidateArrayHandling(JsonMergeHandling arrayHandling)
    {
        if (!Enum.IsDefined(typeof(JsonMergeHandling), arrayHandling))
        {
            throw new ArgumentOutOfRangeException(
                nameof(arrayHandling),
                arrayHandling,
                "Unsupported JSON array merge strategy.");
        }
    }

    private static Dictionary<string, JsonElement> ReadUniqueProperties(JsonElement element)
    {
        var properties = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var property in element.EnumerateObject())
        {
            if (properties.Count == MaximumCollectionItems || !properties.TryAdd(property.Name, property.Value))
            {
                throw new JsonException("JSON object property limit or duplicate property violation.");
            }
        }

        return properties;
    }

    private static void ValidateStructure(JsonElement element, int depth)
    {
        if (depth > MaximumDepth)
        {
            throw new JsonException("JSON structure depth limit exceeded.");
        }

        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var names = new HashSet<string>(StringComparer.Ordinal);
                foreach (var property in element.EnumerateObject())
                {
                    if (names.Count == MaximumCollectionItems || !names.Add(property.Name))
                    {
                        throw new JsonException("JSON object property limit or duplicate property violation.");
                    }

                    ValidateStructure(property.Value, depth + 1);
                }
                break;
            case JsonValueKind.Array:
                if (element.GetArrayLength() > MaximumCollectionItems)
                {
                    throw new JsonException("JSON array item limit exceeded.");
                }

                foreach (var item in element.EnumerateArray())
                {
                    ValidateStructure(item, depth + 1);
                }
                break;
        }
    }

    private static string GetCanonicalKey(JsonElement element)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            WriteCanonical(writer, element, depth: 0);
        }

        return Convert.ToBase64String(buffer.WrittenSpan);
    }

    private static void WriteCanonical(Utf8JsonWriter writer, JsonElement element, int depth)
    {
        if (depth > MaximumDepth)
        {
            throw new JsonException("JSON canonicalization depth limit exceeded.");
        }

        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                var properties = ReadUniqueProperties(element);
                foreach (var property in properties.OrderBy(static item => item.Key, StringComparer.Ordinal))
                {
                    writer.WritePropertyName(property.Key);
                    WriteCanonical(writer, property.Value, depth + 1);
                }
                writer.WriteEndObject();
                break;
            case JsonValueKind.Array:
                if (element.GetArrayLength() > MaximumCollectionItems)
                {
                    throw new JsonException("JSON array item limit exceeded.");
                }
                writer.WriteStartArray();
                foreach (var item in element.EnumerateArray())
                {
                    WriteCanonical(writer, item, depth + 1);
                }
                writer.WriteEndArray();
                break;
            default:
                element.WriteTo(writer);
                break;
        }
    }
}
