using System.Text.Json;
using CSharpExtensions.Core.Json.Enums;
using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Core.Json.Extensions;

/// <summary>
/// Provides Railway Oriented Programming extensions for JSON operations.
/// </summary>
public static class JsonResultExtensions
{
    /// <summary>
    /// Deserializes a JSON string into a <see cref="Result{T}"/>.
    /// </summary>
    public static Result<T> TryDeserialize<T>(this string json, JsonSerializerOptions? options = null)
    {
        if (json.TryDeserialize<T>(out var result, options))
        {
            return result;
        }

        return new Error("Failed to deserialize JSON string.").AsBadRequest("JsonSerializationError", "Invalid JSON data or type mismatch");
    }

    /// <summary>
    /// Deserializes UTF-8 JSON bytes into a <see cref="Result{T}"/>.
    /// </summary>
    public static Result<T> TryDeserialize<T>(this ReadOnlySpan<byte> utf8Json, JsonSerializerOptions? options = null)
    {
        if (utf8Json.TryDeserialize<T>(out var result, options))
        {
            return result;
        }

        return new Error("Failed to deserialize UTF-8 JSON bytes.").AsBadRequest("JsonSerializationError", "Invalid JSON data or type mismatch");
    }

    /// <summary>
    /// Deserializes UTF-8 JSON bytes into a <see cref="Result{T}"/> with structural validation.
    /// </summary>
    public static Result<T> TryDeserializeSafe<T>(this ReadOnlySpan<byte> utf8Json, JsonSerializerOptions? options = null)
    {
        if (utf8Json.TryDeserializeSafe<T>(out var result, options))
        {
            return result;
        }

        return new Error("Failed to deserialize JSON (invalid structure or type mismatch).").AsBadRequest("JsonSafeSerializationError", "The JSON structure is malformed or invalid");
    }

    /// <summary>
    /// Safely gets a property from a <see cref="JsonElement"/> as a <see cref="Result{JsonElement}"/>.
    /// </summary>
    public static Result<JsonElement> GetPropertySafe(this JsonElement element, string propertyName)
    {
        return element.TryGetPropertySafe(propertyName, out var property) 
            ? property 
            : new Error($"Property '{propertyName}' was not found.").AsBadRequest("PropertyNotFoundError", $"Missing property: {propertyName}");
    }

    /// <summary>
    /// Merges two JSON strings using Railway Oriented Programming.
    /// </summary>
    public static Result<string> Merge(this string target, string source, JsonMergeHandling arrayHandling = JsonMergeHandling.Replace)
    {
        try
        {
            return JsonExtensions.Merge(target, source, arrayHandling);
        }
        catch (JsonException exception)
        {
            return new Error("The JSON values could not be merged.")
                .CausedBy(exception)
                .AsBadRequest("JsonMergeError", "Failed to merge JSON values");
        }
    }
}
