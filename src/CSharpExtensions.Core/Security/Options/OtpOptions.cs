namespace CSharpExtensions.Core.Security.Options;

/// <summary>
/// Configuration parameters for generating cryptographically secure One-Time Passwords (OTPs).
/// </summary>
public sealed record OtpOptions
{
    /// <summary>
    /// Gets the desired length of the generated OTP. Must be between 4 and 32. Defaults to 6.
    /// </summary>
    public int Length { get; init; } = 6;

    /// <summary>
    /// Gets the character set allowed in the generated OTP. Defaults to Numeric.
    /// </summary>
    public OtpCharacterSet CharacterSet { get; init; } = OtpCharacterSet.Numeric;

    /// <summary>
    /// Gets a value indicating whether text/letters should be generated in uppercase only.
    /// Improves user experience (UX) and avoids case sensitivity confusion. Defaults to true.
    /// </summary>
    public bool UseUppercaseOnly { get; init; } = true;

    /// <summary>
    /// Gets a value indicating whether easily confusable characters (such as 0/O, 1/I/l, 5/S)
    /// should be excluded from the generated OTP to prevent input errors. Defaults to true.
    /// </summary>
    public bool AvoidAmbiguousCharacters { get; init; } = true;
}
