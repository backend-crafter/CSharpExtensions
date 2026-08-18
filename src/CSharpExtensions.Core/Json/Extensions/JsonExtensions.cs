using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using CSharpExtensions.Core.Json.Enums;

namespace CSharpExtensions.Core.Json.Extensions;

/// <summary>
/// Fluent extension methods for high-performance JSON operations.
/// </summary>
public static class JsonExtensions
{
    private const int MaximumJsonCharacters = 8 * 1024 * 1024;
    private const int MaximumJsonBytes = 16 * 1024 * 1024;
    private const int MaximumCollectionItems = 100_000;

    #region Serialization

    /// <summary>
    /// Serializes an object to a JSON string using standardized defaults.
    /// </summary>
    public static string ToJson<TValue>(this TValue value, JsonSerializerOptions? options = null)
    {
        return JsonSerializer.Serialize(value, options ?? JsonOptions.Default);
    }

    /// <summary>
    /// Serializes an object to UTF-8 encoded bytes using standardized defaults.
    /// </summary>
    public static byte[] ToUtf8Json<TValue>(this TValue value, JsonSerializerOptions? options = null)
    {
        return JsonSerializer.SerializeToUtf8Bytes(value, options ?? JsonOptions.Default);
    }

    #endregion

    #region Deserialization (bool/out)

    /// <summary>
    /// Optimistically attempts to deserialize a JSON string. Returns true if successful.
    /// </summary>
    public static bool TryDeserialize<T>(this string json, [NotNullWhen(true)] out T? result, JsonSerializerOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(json) || json.Length > MaximumJsonCharacters)
        {
            result = default;
            return false;
        }

        try
        {
            result = JsonSerializer.Deserialize<T>(json, options ?? JsonOptions.Default);
            return result is not null;
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            result = default;
            return false;
        }
    }

    /// <summary>
    /// Optimistically attempts to deserialize UTF-8 JSON bytes. Returns true if successful.
    /// </summary>
    public static bool TryDeserialize<T>(this ReadOnlySpan<byte> utf8Json, [NotNullWhen(true)] out T? result, JsonSerializerOptions? options = null)
    {
        if (utf8Json.IsEmpty || utf8Json.Length > MaximumJsonBytes)
        {
            result = default;
            return false;
        }

        try
        {
            result = JsonSerializer.Deserialize<T>(utf8Json, options ?? JsonOptions.Default);
            return result is not null;
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            result = default;
            return false;
        }
    }

    /// <summary>
    /// Pessimistically attempts to deserialize UTF-8 JSON bytes with structure validation. Returns true if successful.
    /// </summary>
    public static bool TryDeserializeSafe<T>(this ReadOnlySpan<byte> utf8Json, [NotNullWhen(true)] out T? result, JsonSerializerOptions? options = null)
    {
        if (utf8Json.IsEmpty || utf8Json.Length > MaximumJsonBytes)
        {
            result = default;
            return false;
        }

        try
        {
            ValidateStructure(utf8Json);
        }
        catch (JsonException)
        {
            result = default;
            return false;
        }

        return utf8Json.TryDeserialize(out result, options);
    }

    #endregion

    #region Merging

    /// <summary>
    /// Merges two JSON strings.
    /// </summary>
    public static string Merge(this string target, string source, JsonMergeHandling arrayHandling = JsonMergeHandling.Replace)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(source);
        JsonMerger.ValidateArrayHandling(arrayHandling);
        if (target.Length > MaximumJsonCharacters || source.Length > MaximumJsonCharacters)
        {
            throw new JsonException("JSON merge input limit exceeded.");
        }

        if (string.IsNullOrWhiteSpace(target))
        {
            ValidateJson(source);
            return source;
        }

        if (string.IsNullOrWhiteSpace(source))
        {
            ValidateJson(target);
            return target;
        }

        var resultBytes = JsonMerger.Merge(System.Text.Encoding.UTF8.GetBytes(target), System.Text.Encoding.UTF8.GetBytes(source), arrayHandling);
        return System.Text.Encoding.UTF8.GetString(resultBytes);
    }

    /// <summary>
    /// Merges two UTF-8 encoded JSON byte spans.
    /// </summary>
    public static byte[] Merge(this ReadOnlySpan<byte> target, ReadOnlySpan<byte> source, JsonMergeHandling arrayHandling = JsonMergeHandling.Replace)
    {
        return JsonMerger.Merge(target, source, arrayHandling);
    }

    #endregion

    #region Utilities

    /// <summary>
    /// Safely gets a property from a <see cref="JsonElement"/>.
    /// </summary>
    public static bool TryGetPropertySafe(this JsonElement element, string propertyName, out JsonElement property)
    {
        property = default;
        return element.ValueKind == JsonValueKind.Object &&
               !string.IsNullOrWhiteSpace(propertyName) &&
               element.TryGetProperty(propertyName, out property);
    }

    private static void ValidateJson(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        if (System.Text.Encoding.UTF8.GetByteCount(value) > MaximumJsonBytes)
        {
            throw new JsonException("JSON merge input limit exceeded.");
        }

        using var document = JsonDocument.Parse(
            value,
            new JsonDocumentOptions { MaxDepth = 64 });
    }

    private static void ValidateStructure(ReadOnlySpan<byte> utf8Json)
    {
        var reader = new Utf8JsonReader(
            utf8Json,
            new JsonReaderOptions
            {
                AllowTrailingCommas = false,
                CommentHandling = JsonCommentHandling.Disallow,
                MaxDepth = 64
            });
        var containers = new Stack<JsonContainerState>();

        while (reader.Read())
        {
            var tokenType = reader.TokenType;
            if (containers.TryPeek(out var parent) &&
                parent.IsArray &&
                tokenType is not JsonTokenType.EndArray)
            {
                parent.Increment();
            }

            switch (tokenType)
            {
                case JsonTokenType.StartObject:
                    containers.Push(new JsonContainerState(isArray: false));
                    break;
                case JsonTokenType.StartArray:
                    containers.Push(new JsonContainerState(isArray: true));
                    break;
                case JsonTokenType.PropertyName:
                    if (!containers.TryPeek(out var objectState) || objectState.IsArray)
                    {
                        throw new JsonException("JSON property appeared outside an object.");
                    }

                    objectState.AddProperty(reader.GetString());
                    break;
                case JsonTokenType.EndObject:
                    PopExpectedContainer(containers, isArray: false);
                    break;
                case JsonTokenType.EndArray:
                    PopExpectedContainer(containers, isArray: true);
                    break;
            }
        }

        if (containers.Count != 0)
        {
            throw new JsonException("JSON structure is incomplete.");
        }
    }

    private static void PopExpectedContainer(Stack<JsonContainerState> containers, bool isArray)
    {
        if (!containers.TryPop(out var state) || state.IsArray != isArray)
        {
            throw new JsonException("JSON container structure is invalid.");
        }
    }

    private sealed class JsonContainerState(bool isArray)
    {
        private readonly HashSet<string>? _propertyNames = isArray
            ? null
            : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private int _itemCount;

        internal bool IsArray { get; } = isArray;

        internal void Increment()
        {
            if (++_itemCount > MaximumCollectionItems)
            {
                throw new JsonException("JSON collection item limit exceeded.");
            }
        }

        internal void AddProperty(string? propertyName)
        {
            Increment();
            if (propertyName is null || !_propertyNames!.Add(propertyName))
            {
                throw new JsonException("Duplicate JSON property names are not supported.");
            }
        }
    }

    #endregion
}
