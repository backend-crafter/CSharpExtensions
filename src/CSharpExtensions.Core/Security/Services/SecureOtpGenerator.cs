using System.Security.Cryptography;
using CSharpExtensions.Core.Security.Interfaces;
using CSharpExtensions.Core.Security.Options;

namespace CSharpExtensions.Core.Security.Services;

/// <summary>
/// Provides a cryptographically secure implementation of the <see cref="IOtpGenerator"/> contract.
/// Eliminates modulo bias and ensures unpredictable, high-entropy OTP codes.
/// </summary>
public sealed class SecureOtpGenerator : IOtpGenerator
{
    private const string NumericDigits = "0123456789";
    private const string UpperAlphaChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private const string MixedAlphaChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private const string SafeNumericDigits = "2346789";
    private const string SafeUpperAlphaChars = "ABCDEFGHJKMNPQRTUVWXYZ";
    private const string SafeMixedAlphaChars = "ABCDEFGHJKMNPQRTUVWXYZabcdefghijkmnopqrstuvwxyz";
    private const string UpperAlphanumericChars = NumericDigits + UpperAlphaChars;
    private const string MixedAlphanumericChars = NumericDigits + MixedAlphaChars;
    private const string SafeUpperAlphanumericChars = SafeNumericDigits + SafeUpperAlphaChars;
    private const string SafeMixedAlphanumericChars = SafeNumericDigits + SafeMixedAlphaChars;

    /// <inheritdoc />
    public string GenerateNumeric(int length = 6)
    {
        return Generate(new OtpOptions
        {
            Length = length,
            CharacterSet = OtpCharacterSet.Numeric,
            AvoidAmbiguousCharacters = false
        });
    }

    /// <inheritdoc />
    public string Generate(OtpOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.Length < 4 || options.Length > 32)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.Length),
                options.Length,
                "OTP length must be between 4 and 32 characters.");
        }

        var characterSet = GetCharacterSet(options);

        if (characterSet.Length < 2)
        {
            throw new ArgumentException("The effective OTP character set must contain at least two characters.", nameof(options));
        }

        Span<char> result = stackalloc char[options.Length];

        for (int i = 0; i < options.Length; i++)
        {
            int index = RandomNumberGenerator.GetInt32(0, characterSet.Length);
            result[i] = characterSet[index];
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

    private static string GetCharacterSet(OtpOptions options)
    {
        return (options.CharacterSet, options.UseUppercaseOnly, options.AvoidAmbiguousCharacters) switch
        {
            (OtpCharacterSet.Numeric, _, false) => NumericDigits,
            (OtpCharacterSet.Numeric, _, true) => SafeNumericDigits,
            (OtpCharacterSet.Alpha, true, false) => UpperAlphaChars,
            (OtpCharacterSet.Alpha, true, true) => SafeUpperAlphaChars,
            (OtpCharacterSet.Alpha, false, false) => MixedAlphaChars,
            (OtpCharacterSet.Alpha, false, true) => SafeMixedAlphaChars,
            (OtpCharacterSet.Alphanumeric, true, false) => UpperAlphanumericChars,
            (OtpCharacterSet.Alphanumeric, true, true) => SafeUpperAlphanumericChars,
            (OtpCharacterSet.Alphanumeric, false, false) => MixedAlphanumericChars,
            (OtpCharacterSet.Alphanumeric, false, true) => SafeMixedAlphanumericChars,
            _ => throw new ArgumentOutOfRangeException(nameof(options.CharacterSet))
        };
    }
}
