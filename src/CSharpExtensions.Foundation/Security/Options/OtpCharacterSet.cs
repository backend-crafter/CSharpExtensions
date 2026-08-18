namespace CSharpExtensions.Foundation.Security.Options;

/// <summary>
/// Specifies the set of characters allowed in generated One-Time Passwords (OTPs).
/// </summary>
public enum OtpCharacterSet
{
    /// <summary>
    /// Codes containing only digits (0-9). Recommended for best user experience.
    /// </summary>
    Numeric,

    /// <summary>
    /// Codes containing only letters (A-Z, a-z).
    /// </summary>
    Alpha,

    /// <summary>
    /// Mixed codes containing both digits and letters.
    /// </summary>
    Alphanumeric
}
