namespace CSharpExtensions.Core.Security.Interfaces;

/// <summary>
/// Service for high-precision data encryption and masking.
/// </summary>
public interface IEncryptionService
{
    /// <summary>
    /// Encrypts the provided string value.
    /// </summary>
    string Encrypt(string value);

    /// <summary>
    /// Decrypts the provided cipher text.
    /// </summary>
    string Decrypt(string cipherText);

    /// <summary>
    /// Attempts to decrypt a ciphertext without returning encrypted input as plaintext on failure.
    /// </summary>
    bool TryDecrypt(string cipherText, out string plaintext)
    {
        try
        {
            plaintext = Decrypt(cipherText);
            return true;
        }
        catch (Exception exception) when (exception is FormatException or System.Security.Cryptography.CryptographicException)
        {
            plaintext = string.Empty;
            return false;
        }
    }

    /// <summary>
    /// Masks a phone number for safe logging or display (e.g., +374*****78).
    /// </summary>
    string MaskPhone(string phoneNumber);

    /// <summary>
    /// Masks an email address (e.g., j***n@example.com).
    /// </summary>
    string MaskEmail(string email);

    /// <summary>
    /// Masks sensitive text, leaving only the specified number of characters visible at the start and end.
    /// </summary>
    string MaskText(string text, int visibleStart = 1, int visibleEnd = 1);
}
