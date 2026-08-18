namespace CSharpExtensions.Foundation.Security.Pii;

/// <summary>
/// Provides high-performance static methods for masking sensitive PII data.
/// </summary>
public static class SensitiveDataMasker
{
    private const int MaximumMaskingInputLength = 128;

    /// <summary>
    /// Masks a string value according to the specified sensitive data type.
    /// </summary>
    /// <param name="value">The string value to mask.</param>
    /// <param name="type">The type of PII masking rules to apply.</param>
    /// <returns>The masked string, or a default placeholder if null/empty.</returns>
    public static string Mask(string? value, SensitiveType type)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        return type switch
        {
            SensitiveType.Phone => value.RedactPhone(),
            SensitiveType.Email => value.RedactEmail(),
            SensitiveType.Card => MaskCard(value),
            _ => value.RedactText()
        };
    }

    private static string MaskCard(string value)
    {
        if (value.Length > MaximumMaskingInputLength)
        {
            return "*****";
        }

        if (value.Length <= 4)
        {
            return new string('*', value.Length);
        }

        var suffix = value.Substring(value.Length - 4);
        return $"{new string('*', value.Length - 4)}{suffix}";
    }
}
