using System.Globalization;

namespace CSharpExtensions.Core.Helpers.Extensions;

/// <summary>
/// High-performance string extension methods.
/// </summary>
public static partial class StringExtensions
{
    /// <summary>
    /// Checks if a string is null, empty, or consists only of white-space characters.
    /// </summary>
    public static bool IsNullOrEmptyOrWhiteSpace(this string? value)
    {
        return string.IsNullOrWhiteSpace(value);
    }

    /// <summary>
    /// Returns a substring from the beginning of the string with the specified length.
    /// Optimized using <see cref="ReadOnlySpan{T}"/>.
    /// </summary>
    public static string Left(this string? value, int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (string.IsNullOrEmpty(value)) return string.Empty;

        var span = value.AsSpan();
        var safeLength = Math.Min(length, span.Length);
        
        return span[..safeLength].ToString();
    }

    /// <summary>
    /// Returns a substring from the end of the string with the specified length.
    /// Optimized using <see cref="ReadOnlySpan{T}"/>.
    /// </summary>
    public static string Right(this string? value, int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (string.IsNullOrEmpty(value)) return string.Empty;

        var span = value.AsSpan();
        var safeLength = Math.Min(length, span.Length);
        
        return span[^safeLength..].ToString();
    }

    /// <summary>
    /// Parses a string to an integer.
    /// </summary>
    public static int ToInt(this string value) => int.Parse(value);

    /// <summary>
    /// Parses a string to a decimal.
    /// </summary>
    public static decimal ToDecimal(this string value) => decimal.Parse(value);

    /// <summary>
    /// Parses a string to an integer using invariant culture.
    /// </summary>
    public static int ToIntInvariant(this string value) =>
        int.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);

    /// <summary>
    /// Parses a string to a decimal using invariant culture.
    /// </summary>
    public static decimal ToDecimalInvariant(this string value) =>
        decimal.Parse(value, NumberStyles.Number, CultureInfo.InvariantCulture);

    /// <summary>
    /// Parses a string to a boolean.
    /// </summary>
    public static bool ToBool(this string value) => bool.Parse(value);

    /// <summary>
    /// Parses a string to a nullable integer.
    /// </summary>
    public static int? ToNullableInt(this string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        return int.TryParse(value, out var result) ? result : null;
    }

    /// <summary>
    /// Parses a string to a nullable decimal.
    /// </summary>
    public static decimal? ToNullableDecimal(this string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        return decimal.TryParse(value, out var result) ? result : null;
    }

    /// <summary>
    /// Parses a string to a nullable boolean.
    /// </summary>
    public static bool? ToNullableBool(this string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        return bool.TryParse(value, out var result) ? result : null;
    }

    /// <summary>
    /// Parses a string to a nullable DateTime.
    /// </summary>
    public static DateTime? ToNullableDateTime(this string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        return DateTime.TryParse(value, out var result) ? result : null;
    }

    /// <summary>
    /// Parses an invariant round-trip timestamp, returning null for empty or invalid input.
    /// </summary>
    public static DateTime? ToNullableDateTimeInvariant(this string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        return DateTime.TryParse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind,
            out var result)
            ? result
            : null;
    }

    /// <summary>
    /// Adds a segment to a base address URI. Robust against missing trailing slashes in baseAddress.
    /// </summary>
    public static Uri AddUrlSegment(this Uri baseAddress, string segment)
    {
        ArgumentNullException.ThrowIfNull(baseAddress);
        ArgumentException.ThrowIfNullOrWhiteSpace(segment);

        if (!baseAddress.IsAbsoluteUri ||
            !string.IsNullOrEmpty(baseAddress.Query) ||
            !string.IsNullOrEmpty(baseAddress.Fragment))
        {
            throw new ArgumentException(
                "The base address must be an absolute URI without a query or fragment.",
                nameof(baseAddress));
        }

        if (Uri.TryCreate(segment, UriKind.Absolute, out _) ||
            segment.StartsWith("//", StringComparison.Ordinal) ||
            segment.StartsWith("\\\\", StringComparison.Ordinal) ||
            segment.Contains('\\') ||
            segment.Contains('#'))
        {
            throw new ArgumentException(
                "The URL segment must be a relative path without a host, backslash, or fragment.",
                nameof(segment));
        }

        var queryIndex = segment.IndexOf('?');
        var relativePath = queryIndex < 0 ? segment : segment[..queryIndex];
        string decodedPath;
        try
        {
            decodedPath = Uri.UnescapeDataString(relativePath);
        }
        catch (UriFormatException exception)
        {
            throw new ArgumentException("The URL segment contains invalid escaping.", nameof(segment), exception);
        }

        if (decodedPath.Contains('\\') ||
            decodedPath.Split('/', StringSplitOptions.RemoveEmptyEntries)
                .Any(pathSegment => pathSegment is "." or ".."))
        {
            throw new ArgumentException(
                "The URL segment cannot escape the configured base path.",
                nameof(segment));
        }

        var basePath = baseAddress.AbsolutePath.EndsWith("/", StringComparison.Ordinal)
            ? baseAddress.AbsolutePath
            : baseAddress.AbsolutePath + "/";
        var baseBuilder = new UriBuilder(baseAddress)
        {
            Path = basePath,
            Query = string.Empty,
            Fragment = string.Empty
        };
        var combined = new Uri(baseBuilder.Uri, segment.TrimStart('/'));

        if (!string.Equals(combined.Scheme, baseAddress.Scheme, StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(combined.IdnHost, baseAddress.IdnHost, StringComparison.OrdinalIgnoreCase) ||
            combined.Port != baseAddress.Port ||
            !combined.AbsolutePath.StartsWith(basePath, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The URL segment cannot replace the destination or escape the configured base path.",
                nameof(segment));
        }

        return combined;
    }
    
    public static string ToCamelCase(this string text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return text;

        // Step 1: Calculate the exact length without using LINQ or delegates.
        var targetLength = text.Count(char.IsLetterOrDigit);

        if (targetLength == 0)
            return string.Empty;

        // Step 2: Allocate memory exactly once for the resulting string.
        return string.Create(targetLength, text, (buffer, str) =>
        {
            var bufferIdx = 0;
            var nextUpper = false;
            var firstWordFound = false;

            // A for loop is guaranteed to compile without any overhead.
            foreach (var ch in str)
            {
                if (char.IsLetterOrDigit(ch))
                {
                    if (!firstWordFound)
                    {
                        // The first character is always lowercase
                        buffer[bufferIdx++] = char.ToLowerInvariant(ch);
                        firstWordFound = true;
                    }
                    else if (nextUpper)
                    {
                        buffer[bufferIdx++] = char.ToUpperInvariant(ch);
                        nextUpper = false;
                    }
                    else
                    {
                        buffer[bufferIdx++] = char.ToLowerInvariant(ch);
                    }
                }
                else if (firstWordFound)
                {
                    nextUpper = true;
                }
            }
        });
    }
}
