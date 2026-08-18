using System.Text;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.Foundation.Security.Options;

/// <summary>
/// Validates encryption configuration before the first cryptographic operation.
/// </summary>
public sealed class EncryptionOptionsValidator : IValidateOptions<EncryptionOptions>
{
    private const int MaximumPurposeLength = 128;
    private const int MaximumKeyIdLength = 64;

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, EncryptionOptions options)
    {
        if (options is null)
        {
            return ValidateOptionsResult.Fail("Encryption options are required.");
        }

        var failures = new List<string>();

        var requiresLegacyMaterial = options.WriteMode == EncryptionWriteMode.LegacyCbc ||
                                     options.AllowLegacyDecryption;
        if (requiresLegacyMaterial)
        {
            ValidateKeyMaterial(options.Key, "Key", failures);

            if (Encoding.UTF8.GetByteCount(options.Iv ?? string.Empty) != 16)
            {
                failures.Add("EncryptionOptions.Iv must contain exactly 16 UTF-8 bytes for legacy CBC reads.");
            }
        }

        if (!Enum.IsDefined(options.WriteMode))
        {
            failures.Add("EncryptionOptions.WriteMode is invalid.");
        }

        if (options.WriteMode == EncryptionWriteMode.LegacyCbc && !options.AllowLegacyDecryption)
        {
            failures.Add("Legacy CBC writes cannot be combined with disabled legacy decryption.");
        }

        if (!IsSafeKeyId(options.ActiveKeyId))
        {
            failures.Add($"EncryptionOptions.ActiveKeyId must contain 1 to {MaximumKeyIdLength} ASCII letters, digits, '.', '_' or '-'.");
        }

        if (string.IsNullOrWhiteSpace(options.Purpose) || options.Purpose.Length > MaximumPurposeLength)
        {
            failures.Add($"EncryptionOptions.Purpose must contain 1 to {MaximumPurposeLength} characters.");
        }

        if (options.MaxPlaintextBytes is < 1 or > 16 * 1024 * 1024)
        {
            failures.Add("EncryptionOptions.MaxPlaintextBytes must be between 1 byte and 16 MiB.");
        }

        if (options.KeyRing is null)
        {
            failures.Add("EncryptionOptions.KeyRing cannot be null.");
        }
        else
        {
            foreach (var (keyId, keyMaterial) in options.KeyRing)
            {
                if (!IsSafeKeyId(keyId))
                {
                    failures.Add("EncryptionOptions.KeyRing contains an invalid key identifier.");
                    continue;
                }

                ValidateKeyMaterial(keyMaterial, $"KeyRing[{keyId}]", failures);
            }
        }

        if (options.WriteMode == EncryptionWriteMode.AesGcmV2 &&
            options.KeyRing is { Count: > 0 } &&
            !options.KeyRing.ContainsKey(options.ActiveKeyId))
        {
            failures.Add("EncryptionOptions.ActiveKeyId is not present in EncryptionOptions.KeyRing.");
        }

        if (options.WriteMode == EncryptionWriteMode.AesGcmV2 &&
            !options.AllowLegacyDecryption &&
            (options.KeyRing is not { Count: > 0 } || !options.KeyRing.ContainsKey(options.ActiveKeyId)))
        {
            failures.Add("Authenticated-only encryption requires ActiveKeyId to be present in EncryptionOptions.KeyRing.");
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }

    internal static byte[] DecodeKeyMaterial(string keyMaterial)
    {
        if (keyMaterial.StartsWith("base64:", StringComparison.OrdinalIgnoreCase))
        {
            return Convert.FromBase64String(keyMaterial[7..]);
        }

        return Encoding.UTF8.GetBytes(keyMaterial);
    }

    private static void ValidateKeyMaterial(string? keyMaterial, string fieldName, ICollection<string> failures)
    {
        if (string.IsNullOrEmpty(keyMaterial))
        {
            failures.Add($"EncryptionOptions.{fieldName} is required.");
            return;
        }

        try
        {
            var keyBytes = DecodeKeyMaterial(keyMaterial);
            try
            {
                if (keyBytes.Length is not (16 or 24 or 32))
                {
                    failures.Add($"EncryptionOptions.{fieldName} must decode to 16, 24, or 32 bytes.");
                }
            }
            finally
            {
                System.Security.Cryptography.CryptographicOperations.ZeroMemory(keyBytes);
            }
        }
        catch (FormatException)
        {
            failures.Add($"EncryptionOptions.{fieldName} contains invalid Base64 key material.");
        }
    }

    internal static bool IsSafeKeyId(string? keyId)
    {
        if (string.IsNullOrWhiteSpace(keyId) || keyId.Length > MaximumKeyIdLength)
        {
            return false;
        }

        foreach (var character in keyId)
        {
            if (!char.IsAsciiLetterOrDigit(character) && character is not ('.' or '_' or '-'))
            {
                return false;
            }
        }

        return true;
    }
}
