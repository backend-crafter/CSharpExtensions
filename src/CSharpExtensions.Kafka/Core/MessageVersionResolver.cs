using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace CSharpExtensions.Kafka.Core;

/// <summary>
/// Helper utility to resolve message versions from classes or JSON payloads.
/// </summary>
public static class MessageVersionResolver
{
    internal const int MaximumSupportedVersion = 1000;
    private const int MaxSchemaKeyLength = 256;
    private const int MaxVersionProbeBytes = 1024 * 1024;
    private static readonly Regex VersionSuffixRegex = new(
        @"(?:(?:\.v)|V)(?<version>[0-9]+)$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled,
        TimeSpan.FromMilliseconds(100));

    /// <summary>
    /// Obtains the integer Version of a message class using reflection.
    /// Returns 1 if not declared.
    /// </summary>
    public static int GetMessageVersion<TMessage>() where TMessage : class
    {
        return GetMessageVersion(typeof(TMessage));
    }

    /// <summary>
    /// Obtains the integer Version of a message type without invoking its constructor.
    /// Returns 1 if not declared.
    /// </summary>
    public static int GetMessageVersion(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        if (TryGetStaticVersion(messageType, out var staticVersion))
        {
            return ValidateVersion(staticVersion);
        }

        var instanceProperty = messageType.GetProperty("Version", BindingFlags.Public | BindingFlags.Instance);
        if (instanceProperty?.PropertyType != typeof(int) || instanceProperty.GetMethod is null)
        {
            return 1;
        }

        object instance;
        try
        {
            instance = RuntimeHelpers.GetUninitializedObject(messageType);
        }
        catch (Exception exception) when (exception is ArgumentException or MemberAccessException or NotSupportedException)
        {
            throw new InvalidOperationException(
                $"Kafka message version metadata for type '{messageType.FullName}' cannot be read without invoking its constructor.");
        }

        try
        {
            if (TryGetInstanceVersion(messageType, instance, out var instanceVersion))
            {
                return ValidateVersion(instanceVersion);
            }
        }
        catch (Exception exception) when (exception is TargetInvocationException or MethodAccessException)
        {
            throw new InvalidOperationException(
                $"Kafka message version metadata for type '{messageType.FullName}' could not be evaluated safely.");
        }

        return 1;
    }

    /// <summary>
    /// Obtains the integer Version of a message instance using reflection.
    /// </summary>
    public static int GetMessageVersion(object message)
    {
        if (message is null) return 1;
        var messageType = message.GetType();
        if (TryGetStaticVersion(messageType, out var staticVersion))
        {
            return ValidateVersion(staticVersion);
        }

        if (TryGetInstanceVersion(messageType, message, out var instanceVersion))
        {
            return ValidateVersion(instanceVersion);
        }

        return 1;
    }

    private static bool TryGetStaticVersion(Type messageType, out int version)
    {
        var field = messageType.GetField(
            "Version",
            BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
        if (field is { FieldType: not null, IsLiteral: true } &&
            field.FieldType == typeof(int) &&
            field.GetRawConstantValue() is int constantVersion)
        {
            version = constantVersion;
            return true;
        }

        var property = messageType.GetProperty(
            "Version",
            BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
        if (property?.PropertyType == typeof(int) &&
            property.GetMethod is { IsStatic: true } &&
            property.GetValue(null) is int staticVersion)
        {
            version = staticVersion;
            return true;
        }

        version = default;
        return false;
    }

    private static bool TryGetInstanceVersion(Type messageType, object instance, out int version)
    {
        var property = messageType.GetProperty("Version", BindingFlags.Public | BindingFlags.Instance);
        if (property?.PropertyType == typeof(int) &&
            property.GetMethod is not null &&
            property.GetValue(instance) is int instanceVersion)
        {
            version = instanceVersion;
            return true;
        }

        version = default;
        return false;
    }

    /// <summary>
    /// Resolves the source schema key from the consumed header value,
    /// falling back to version detection from the raw JSON payload.
    /// </summary>
    public static string ResolveSourceSchemaKey(string schemaVersionKey, string rawPayloadJson)
    {
        if (string.IsNullOrWhiteSpace(schemaVersionKey))
        {
            return string.Empty;
        }

        if (schemaVersionKey.Length > MaxSchemaKeyLength || ContainsControlCharacter(schemaVersionKey))
        {
            throw new FormatException("Kafka schema key is invalid.");
        }

        var suffixMatch = VersionSuffixRegex.Match(schemaVersionKey);
        if (suffixMatch.Success)
        {
            if (!int.TryParse(
                    suffixMatch.Groups["version"].Value,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var suffixVersion))
            {
                throw new FormatException("Kafka schema version suffix is invalid.");
            }

            ValidateVersion(suffixVersion);
            return schemaVersionKey;
        }

        ArgumentNullException.ThrowIfNull(rawPayloadJson);
        if (Encoding.UTF8.GetByteCount(rawPayloadJson) > MaxVersionProbeBytes)
        {
            throw new FormatException("Kafka payload is too large for schema-version probing.");
        }

        try
        {
            using var doc = JsonDocument.Parse(
                rawPayloadJson,
                new JsonDocumentOptions { MaxDepth = 32 });
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException("Kafka payload root must be a JSON object for schema-version probing.");
            }

            if (doc.RootElement.TryGetProperty("version", out var versionProperty))
            {
                if (versionProperty.ValueKind != JsonValueKind.Number
                    || !versionProperty.TryGetInt32(out var version))
                {
                    throw new FormatException("Kafka payload version is invalid.");
                }

                return $"{schemaVersionKey}.v{ValidateVersion(version)}";
            }

            if (doc.RootElement.TryGetProperty("schemaVersion", out var legacyProp) && legacyProp.ValueKind == JsonValueKind.String)
            {
                var value = legacyProp.GetString();
                if (!TryParseLegacyMajorVersion(value, out var majorVersion))
                {
                    throw new FormatException("Kafka legacy schemaVersion is invalid.");
                }

                return $"{schemaVersionKey}.v{ValidateVersion(majorVersion)}";
            }

            if (doc.RootElement.TryGetProperty("schemaVersion", out _))
            {
                throw new FormatException("Kafka legacy schemaVersion is invalid.");
            }
        }
        catch (JsonException exception)
        {
            throw new FormatException("Kafka payload is invalid JSON for schema-version probing.", exception);
        }

        return $"{schemaVersionKey}.v1";
    }

    /// <summary>
    /// Attempts to resolve a source schema key without falling back when an explicitly supplied version is invalid.
    /// </summary>
    public static bool TryResolveSourceSchemaKey(
        string schemaVersionKey,
        string rawPayloadJson,
        out string sourceSchemaKey)
    {
        try
        {
            sourceSchemaKey = ResolveSourceSchemaKey(schemaVersionKey, rawPayloadJson);
            return true;
        }
        catch (Exception exception) when (exception is FormatException or ArgumentNullException or InvalidOperationException)
        {
            sourceSchemaKey = string.Empty;
            return false;
        }
    }

    internal static bool HasVersionSuffix(string schemaKey)
    {
        if (string.IsNullOrWhiteSpace(schemaKey) || schemaKey.Length > MaxSchemaKeyLength)
        {
            return false;
        }

        var match = VersionSuffixRegex.Match(schemaKey);
        return match.Success
            && int.TryParse(
                match.Groups["version"].Value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var version)
            && version is >= 1 and <= MaximumSupportedVersion;
    }

    private static int ValidateVersion(int version)
    {
        if (version is < 1 or > MaximumSupportedVersion)
        {
            throw new InvalidOperationException(
                $"Kafka message version must be between 1 and {MaximumSupportedVersion}.");
        }

        return version;
    }

    private static bool TryParseLegacyMajorVersion(string? value, out int majorVersion)
    {
        majorVersion = default;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var separatorIndex = value.IndexOf('.');
        var major = separatorIndex < 0 ? value : value[..separatorIndex];
        if (!int.TryParse(major, NumberStyles.None, CultureInfo.InvariantCulture, out majorVersion))
        {
            return false;
        }

        if (separatorIndex >= 0)
        {
            var hasDigitInSegment = false;
            for (var index = separatorIndex + 1; index < value.Length; index++)
            {
                var character = value[index];
                if (char.IsAsciiDigit(character))
                {
                    hasDigitInSegment = true;
                    continue;
                }

                if (character != '.' || !hasDigitInSegment)
                {
                    return false;
                }

                hasDigitInSegment = false;
            }

            if (!hasDigitInSegment) return false;
        }

        return true;
    }

    private static bool ContainsControlCharacter(string value)
    {
        foreach (var character in value)
        {
            if (char.IsControl(character))
            {
                return true;
            }
        }

        return false;
    }
}
