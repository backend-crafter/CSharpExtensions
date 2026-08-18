using CSharpExtensions.Foundation.Security.Options;

namespace CSharpExtensions.Foundation.Security.Interfaces;

/// <summary>
/// Defines a contract for generating cryptographically secure One-Time Passwords (OTPs).
/// </summary>
public interface IOtpGenerator
{
    /// <summary>
    /// Generates a cryptographically secure One-Time Password based on the provided configuration options.
    /// </summary>
    /// <param name="options">The custom options to configure the OTP generator.</param>
    /// <returns>A securely generated random OTP string matching the specification.</returns>
    string Generate(OtpOptions options);

    /// <summary>
    /// Shorthand method to generate a standard, cryptographically secure numeric OTP of the specified length.
    /// </summary>
    /// <param name="length">The length of the generated code (must be between 4 and 32). Defaults to 6.</param>
    /// <returns>A string containing only numeric digits.</returns>
    string GenerateNumeric(int length = 6);
}
