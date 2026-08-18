namespace CSharpExtensions.Core.Helpers;

/// <summary>
/// Validates opaque identifiers before they cross HTTP and logging boundaries.
/// </summary>
public static class BoundedIdentifier
{
    public const int DefaultMaximumLength = 128;

    public static bool TryNormalize(
        string? value,
        out string normalized,
        int maximumLength = DefaultMaximumLength)
    {
        normalized = string.Empty;
        if (maximumLength <= 0 || string.IsNullOrWhiteSpace(value) || value.Length > maximumLength)
        {
            return false;
        }

        foreach (var character in value)
        {
            if (!(char.IsAsciiLetterOrDigit(character) || character is '-' or '_' or '.' or ':' or '/' or '~'))
            {
                return false;
            }
        }

        normalized = value;
        return true;
    }
}
