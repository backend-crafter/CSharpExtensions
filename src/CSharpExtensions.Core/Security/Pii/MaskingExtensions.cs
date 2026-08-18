namespace CSharpExtensions.Core.Security.Pii;

/// <summary>
/// General-purpose string masking extensions for PII protection (e.g. Phone, Email, and Text masking).
/// </summary>
public static class MaskingExtensions
{
    private const int MaximumEmailInputLength = 320;
    private const int MaximumRedactionInputLength = 128;

    /// <summary>
    /// Safely masks a phone number for logs or UI display (e.g. +374*****78).
    /// </summary>
    public static string MaskPhone(this string? phoneNumber)
    {
        if (string.IsNullOrEmpty(phoneNumber))
            return string.Empty;

        if (phoneNumber.Length < 7)
            return new string('*', phoneNumber.Length);

        // Optimized masking pattern: first 4 chars visible, middle masked, last 2 visible.
        return $"{phoneNumber[..4]}*****{phoneNumber[^2..]}";
    }

    /// <summary>
    /// Redacts a phone value for logs without exposing its prefix.
    /// </summary>
    public static string RedactPhone(this string? phoneNumber)
    {
        if (string.IsNullOrEmpty(phoneNumber))
        {
            return string.Empty;
        }

        if (phoneNumber.Length <= 2)
        {
            return new string('*', phoneNumber.Length);
        }

        if (phoneNumber.Length > MaximumRedactionInputLength)
        {
            return "*****";
        }

        return $"{new string('*', phoneNumber.Length - 2)}{phoneNumber[^2..]}";
    }

    /// <summary>
    /// Safely masks an email address protecting privacy (e.g. s***y@domain.com).
    /// </summary>
    public static string MaskEmail(this string? email)
    {
        if (string.IsNullOrWhiteSpace(email)) 
            return string.Empty;

        if (email.Length > MaximumEmailInputLength)
            return "***@***";

        var separatorIndex = email.IndexOf('@');
        if (separatorIndex <= 0 || separatorIndex != email.LastIndexOf('@') || separatorIndex == email.Length - 1)
            return email.MaskText();

        var name = email.AsSpan(0, separatorIndex);
        var domain = email.AsSpan(separatorIndex + 1);

        if (name.Length <= 2) 
            return $"*@{domain}";

        return $"{name[0]}***{name[^1]}@{domain}";
    }

    /// <summary>
    /// Masks a generic sensitive string leaving a specified number of characters visible at the boundaries.
    /// </summary>
    public static string MaskText(this string? text, int visibleStart = 1, int visibleEnd = 1)
    {
        if (visibleStart < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(visibleStart), "Visible character count cannot be negative.");
        }

        if (visibleEnd < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(visibleEnd), "Visible character count cannot be negative.");
        }

        if (string.IsNullOrWhiteSpace(text)) 
            return string.Empty;

        if (visibleStart >= text.Length || visibleEnd >= text.Length - visibleStart)
        {
            return new string('*', text.Length);
        }

        var suffix = visibleEnd == 0 ? string.Empty : text[^visibleEnd..];
        return $"{text[..visibleStart]}***{suffix}";
    }

    /// <summary>
    /// Redacts an email address for logs without exposing the local part or domain.
    /// </summary>
    public static string RedactEmail(this string? email)
    {
        return string.IsNullOrWhiteSpace(email) ? string.Empty : "***@***";
    }

    /// <summary>
    /// Fully redacts arbitrary sensitive text for logs.
    /// </summary>
    public static string RedactText(this string? text)
    {
        return string.IsNullOrWhiteSpace(text) ? string.Empty : "*****";
    }
}
