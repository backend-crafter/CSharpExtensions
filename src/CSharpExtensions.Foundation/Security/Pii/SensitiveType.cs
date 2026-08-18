namespace CSharpExtensions.Foundation.Security.Pii;

/// <summary>
/// Specifies the type of PII formatting for masking.
/// </summary>
public enum SensitiveType
{
    /// <summary>
    /// Mask everything.
    /// </summary>
    Text,

    /// <summary>
    /// Mask phone number (e.g., +374*****78).
    /// </summary>
    Phone,

    /// <summary>
    /// Mask email address (e.g., jo*****@domain.com).
    /// </summary>
    Email,

    /// <summary>
    /// Mask bank card/PAN (e.g., ************4321).
    /// </summary>
    Card
}
