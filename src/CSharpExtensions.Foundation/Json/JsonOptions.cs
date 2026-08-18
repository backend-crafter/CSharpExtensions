using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpExtensions.Foundation.Json.Policies;

namespace CSharpExtensions.Foundation.Json;

/// <summary>
/// Provides high-performance, standardized JSON serialization options for the ecosystem.
/// </summary>
public static class JsonOptions
{
    /// <summary>
    /// The compatibility <see cref="JsonSerializerOptions"/> instance used by existing internal contracts.
    /// Use <see cref="ExternalStrict"/> for untrusted external input.
    /// </summary>
    public static JsonSerializerOptions Default { get; } = CreateFrozen(CreateCamelCase());

    /// <summary>
    /// A pre-configured <see cref="JsonSerializerOptions"/> instance using camelCase.
    /// </summary>
    public static JsonSerializerOptions CamelCase { get; } = CreateFrozen(CreateCamelCase());

    /// <summary>
    /// A pre-configured <see cref="JsonSerializerOptions"/> instance using snake_case (lower).
    /// </summary>
    public static JsonSerializerOptions SnakeCase { get; } = CreateFrozen(CreateSnakeCase());

    /// <summary>
    /// A pre-configured <see cref="JsonSerializerOptions"/> instance using lowercase (no separators).
    /// </summary>
    public static JsonSerializerOptions LowerCase { get; } = CreateFrozen(CreateLowerCase());

    /// <summary>
    /// A pre-configured <see cref="JsonSerializerOptions"/> instance using SNAKE_CASE (UPPER).
    /// </summary>
    public static JsonSerializerOptions SnakeCaseUpper { get; } = CreateFrozen(CreateSnakeCaseUpper());

    /// <summary>
    /// A pre-configured <see cref="JsonSerializerOptions"/> instance using kebab-case (lower).
    /// </summary>
    public static JsonSerializerOptions KebabCase { get; } = CreateFrozen(CreateKebabCase());

    /// <summary>
    /// A pre-configured <see cref="JsonSerializerOptions"/> instance using KEBAB-CASE (UPPER).
    /// </summary>
    public static JsonSerializerOptions KebabCaseUpper { get; } = CreateFrozen(CreateKebabCaseUpper());

    /// <summary>
    /// A pre-configured <see cref="JsonSerializerOptions"/> instance using PascalCase (default C# behavior).
    /// </summary>
    public static JsonSerializerOptions PascalCase { get; } = CreateFrozen(CreatePascalCase());

    /// <summary>
    /// A strict profile for untrusted external JSON. Unknown members, comments,
    /// trailing commas, and numbers encoded as strings are rejected.
    /// </summary>
    public static JsonSerializerOptions ExternalStrict { get; } = CreateFrozen(CreateExternalStrict());

    /// <summary>
    /// A bounded HTTP response profile with strict JSON syntax, numbers, and enums.
    /// Unknown members remain allowed so additive response-contract changes are forward compatible.
    /// </summary>
    public static JsonSerializerOptions HttpResponse { get; } = CreateFrozen(CreateHttpResponse());

    /// <summary>
    /// A compatibility profile for existing Kafka payloads. This is intentionally
    /// separate from <see cref="ExternalStrict"/> so wire changes are explicit.
    /// </summary>
    public static JsonSerializerOptions KafkaCompatible { get; } = CreateFrozen(CreateCamelCase());

    /// <summary>
    /// Creates a new instance of <see cref="JsonSerializerOptions"/> with camelCase settings.
    /// </summary>
    public static JsonSerializerOptions CreateCamelCase() => CreateDefault(JsonNamingPolicy.CamelCase);

    /// <summary>
    /// Creates a new instance of <see cref="JsonSerializerOptions"/> with snake_case (lower) settings.
    /// </summary>
    public static JsonSerializerOptions CreateSnakeCase() => CreateDefault(JsonNamingPolicy.SnakeCaseLower);

    /// <summary>
    /// Creates a new instance of <see cref="JsonSerializerOptions"/> with SNAKE_CASE (UPPER) settings.
    /// </summary>
    public static JsonSerializerOptions CreateSnakeCaseUpper() => CreateDefault(JsonNamingPolicy.SnakeCaseUpper);

    /// <summary>
    /// Creates a new instance of <see cref="JsonSerializerOptions"/> with kebab-case (lower) settings.
    /// </summary>
    public static JsonSerializerOptions CreateKebabCase() => CreateDefault(JsonNamingPolicy.KebabCaseLower);

    /// <summary>
    /// Creates a new instance of <see cref="JsonSerializerOptions"/> with KEBAB-CASE (UPPER) settings.
    /// </summary>
    public static JsonSerializerOptions CreateKebabCaseUpper() => CreateDefault(JsonNamingPolicy.KebabCaseUpper);

    /// <summary>
    /// Creates a new instance of <see cref="JsonSerializerOptions"/> with lowercase (no separators) settings.
    /// </summary>
    public static JsonSerializerOptions CreateLowerCase() => CreateDefault(LowerCaseNamingPolicy.Instance);

    /// <summary>
    /// Creates a new instance of <see cref="JsonSerializerOptions"/> with PascalCase settings (null policy).
    /// </summary>
    public static JsonSerializerOptions CreatePascalCase() => CreateDefault(null);

    /// <summary>
    /// Creates a mutable strict profile suitable for customization during startup.
    /// </summary>
    public static JsonSerializerOptions CreateExternalStrict()
    {
        var options = CreateDefault(JsonNamingPolicy.CamelCase, allowIntegerEnumValues: false);
        options.AllowTrailingCommas = false;
        options.ReadCommentHandling = JsonCommentHandling.Disallow;
        options.PropertyNameCaseInsensitive = false;
        options.NumberHandling = JsonNumberHandling.Strict;
        options.UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow;
        options.Encoder = JavaScriptEncoder.Default;
        options.MaxDepth = 64;
        return options;
    }

    /// <summary>
    /// Creates a mutable HTTP response profile suitable for customization during startup.
    /// </summary>
    public static JsonSerializerOptions CreateHttpResponse()
    {
        var options = CreateDefault(JsonNamingPolicy.CamelCase, allowIntegerEnumValues: false);
        options.AllowTrailingCommas = false;
        options.ReadCommentHandling = JsonCommentHandling.Disallow;
        options.PropertyNameCaseInsensitive = true;
        options.NumberHandling = JsonNumberHandling.Strict;
        options.UnmappedMemberHandling = JsonUnmappedMemberHandling.Skip;
        options.Encoder = JavaScriptEncoder.Default;
        options.MaxDepth = 64;
        return options;
    }

    private static JsonSerializerOptions CreateDefault(
        JsonNamingPolicy? namingPolicy,
        bool allowIntegerEnumValues = true)
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = namingPolicy,
            DictionaryKeyPolicy = namingPolicy,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
            AllowTrailingCommas = true,
            ReadCommentHandling = JsonCommentHandling.Skip,
            PropertyNameCaseInsensitive = true,
            NumberHandling = JsonNumberHandling.AllowReadingFromString,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        // Add string enum converter with the appropriate naming policy
        options.Converters.Add(new JsonStringEnumConverter(namingPolicy, allowIntegerEnumValues));

        return options;
    }

    private static JsonSerializerOptions CreateFrozen(JsonSerializerOptions options)
    {
        options.MakeReadOnly(populateMissingResolver: true);
        return options;
    }
}
