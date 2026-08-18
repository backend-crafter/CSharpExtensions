using System.Security.Cryptography;

namespace CSharpExtensions.Foundation.Security.Extensions;

/// <summary>
/// Provides cryptographically secure secret generation for SSO clients, API keys, and other sensitive tokens.
/// Uses <see cref="RandomNumberGenerator"/> to ensure unpredictable, high-entropy output.
/// </summary>
public static class SecretGenerator
{
    private const string AlphanumericCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private const string SpecialCharacters = "!@#$%^&*_-+=";

    private const int DefaultLength = 64;
    private const int MinimumLength = 32;
    private const int MaximumLength = 256;

    /// <summary>
    /// Generates a cryptographically secure alphanumeric secret of the specified length.
    /// Suitable for SSO client secrets, API keys, and bearer tokens where special characters may cause encoding issues.
    /// </summary>
    /// <param name="length">The desired length of the generated secret. Must be between 32 and 256. Defaults to 64.</param>
    /// <returns>A cryptographically secure random string containing only alphanumeric characters.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when length is less than 32 or greater than 256.</exception>
    public static string GenerateAlphanumeric(int length = DefaultLength)
    {
        ValidateLength(length);
        return Generate(length, AlphanumericCharacters);
    }

    /// <summary>
    /// Generates a cryptographically secure secret containing alphanumeric and special characters.
    /// Provides higher entropy per character. Suitable for internal secrets that won't be passed in URLs or headers.
    /// </summary>
    /// <param name="length">The desired length of the generated secret. Must be between 32 and 256. Defaults to 64.</param>
    /// <returns>A cryptographically secure random string containing alphanumeric and special characters.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when length is less than 32 or greater than 256.</exception>
    public static string GenerateComplex(int length = DefaultLength)
    {
        ValidateLength(length);
        return Generate(length, AlphanumericCharacters + SpecialCharacters);
    }

    /// <summary>
    /// Generates a cryptographically secure secret and returns both the plain-text secret and its SHA-256 hash.
    /// This is the recommended method for SSO client registration, where the hash is stored in the database
    /// and the plain-text secret is communicated to the client exactly once.
    /// </summary>
    /// <param name="length">The desired length of the generated secret. Must be between 32 and 256. Defaults to 64.</param>
    /// <returns>A tuple containing the plain-text secret and its SHA-256 hash (lowercase hex).</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when length is less than 32 or greater than 256.</exception>
    public static (string Secret, string Hash) GenerateWithHash(int length = DefaultLength)
    {
        var secret = GenerateAlphanumeric(length);
        var hash = HashGenerator.ComputeHash(secret);
        return (secret, hash);
    }

    private static string Generate(int length, string characterSet)
    {
        Span<char> result = stackalloc char[length];

        for (int i = 0; i < length; i++)
        {
            result[i] = characterSet[RandomNumberGenerator.GetInt32(characterSet.Length)];
        }

        try
        {
            return new string(result);
        }
        finally
        {
            result.Clear();
        }
    }

    private static void ValidateLength(int length)
    {
        if (length < MinimumLength || length > MaximumLength)
        {
            throw new ArgumentOutOfRangeException(
                nameof(length),
                length,
                $"Secret length must be between {MinimumLength} and {MaximumLength} characters.");
        }
    }
}
