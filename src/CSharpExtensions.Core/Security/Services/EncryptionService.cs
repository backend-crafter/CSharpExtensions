using System.Security.Cryptography;
using System.Text;
using CSharpExtensions.Core.Security.Interfaces;
using CSharpExtensions.Core.Security.Options;
using CSharpExtensions.Core.Security.Pii;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.Core.Security.Services;

/// <summary>
/// Provides legacy-compatible AES-CBC and versioned authenticated AES-GCM encryption.
/// </summary>
public sealed class EncryptionService : IEncryptionService
{
    private const string AesGcmEnvelopePrefix = "v2";
    private const int AesGcmNonceSize = 12;
    private const int AesGcmTagSize = 16;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    private readonly EncryptionOptions _options;

    public EncryptionService(IOptions<EncryptionOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var configuredOptions = options.Value ?? throw new OptionsValidationException(
            nameof(EncryptionOptions),
            typeof(EncryptionOptions),
            ["Encryption options are required."]);

        _options = CreateSnapshot(configuredOptions);

        var validation = new EncryptionOptionsValidator().Validate(null, _options);
        if (validation.Failed)
        {
            throw new OptionsValidationException(
                nameof(EncryptionOptions),
                typeof(EncryptionOptions),
                validation.Failures);
        }
    }

    /// <inheritdoc />
    public string Encrypt(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return value;
        }

        EnsurePlaintextSize(value);

        return _options.WriteMode switch
        {
            EncryptionWriteMode.LegacyCbc => EncryptLegacyCbc(value),
            EncryptionWriteMode.AesGcmV2 => EncryptAesGcm(value),
            _ => throw new CryptographicException("The configured encryption write mode is unsupported.")
        };
    }

    /// <inheritdoc />
    public string Decrypt(string cipherText)
    {
        if (string.IsNullOrWhiteSpace(cipherText))
        {
            return cipherText;
        }

        if (TryDecrypt(cipherText, out var plaintext))
        {
            return plaintext;
        }

        throw new CryptographicException("Ciphertext authentication or decryption failed.");
    }

    /// <inheritdoc />
    public bool TryDecrypt(string cipherText, out string plaintext)
    {
        if (string.IsNullOrWhiteSpace(cipherText))
        {
            plaintext = cipherText;
            return true;
        }

        try
        {
            if (cipherText.StartsWith(AesGcmEnvelopePrefix + ":", StringComparison.Ordinal))
            {
                plaintext = DecryptAesGcm(cipherText);
            }
            else
            {
                if (!_options.AllowLegacyDecryption)
                {
                    throw new CryptographicException("Legacy CBC decryption is disabled.");
                }

                plaintext = DecryptLegacyCbc(cipherText);
            }

            return true;
        }
        catch (Exception exception) when (
            exception is FormatException or CryptographicException or ArgumentException)
        {
            plaintext = string.Empty;
            return false;
        }
    }

    /// <inheritdoc />
    public string MaskPhone(string phoneNumber) => phoneNumber.MaskPhone();

    /// <inheritdoc />
    public string MaskEmail(string email) => email.MaskEmail();

    /// <inheritdoc />
    public string MaskText(string text, int visibleStart = 1, int visibleEnd = 1)
        => text.MaskText(visibleStart, visibleEnd);

    private string EncryptLegacyCbc(string value)
    {
        var key = GetLegacyKey();
        var plaintext = StrictUtf8.GetBytes(value);

        try
        {
            using var aes = Aes.Create();
            aes.Key = key;
            aes.GenerateIV();

            using var encryptor = aes.CreateEncryptor();
            var ciphertext = encryptor.TransformFinalBlock(plaintext, 0, plaintext.Length);
            try
            {
                return $"{Convert.ToBase64String(aes.IV)}:{Convert.ToBase64String(ciphertext)}";
            }
            finally
            {
                CryptographicOperations.ZeroMemory(ciphertext);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
            CryptographicOperations.ZeroMemory(plaintext);
        }
    }

    private string EncryptAesGcm(string value)
    {
        var keyId = _options.ActiveKeyId;
        var key = GetKey(keyId);
        var plaintext = StrictUtf8.GetBytes(value);
        var nonce = RandomNumberGenerator.GetBytes(AesGcmNonceSize);
        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[AesGcmTagSize];
        var aad = CreateAad(keyId);

        try
        {
            using var aes = new AesGcm(key, AesGcmTagSize);
            aes.Encrypt(nonce, plaintext, ciphertext, tag, aad);

            return string.Join(
                ':',
                AesGcmEnvelopePrefix,
                keyId,
                Convert.ToBase64String(nonce),
                Convert.ToBase64String(ciphertext),
                Convert.ToBase64String(tag));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
            CryptographicOperations.ZeroMemory(plaintext);
            CryptographicOperations.ZeroMemory(nonce);
            CryptographicOperations.ZeroMemory(ciphertext);
            CryptographicOperations.ZeroMemory(tag);
            CryptographicOperations.ZeroMemory(aad);
        }
    }

    private string DecryptAesGcm(string envelope)
    {
        EnsureCiphertextSize(envelope);
        const int payloadStart = 3;
        var keySeparatorIndex = envelope.IndexOf(':', payloadStart);
        var nonceSeparatorIndex = keySeparatorIndex < 0
            ? -1
            : envelope.IndexOf(':', keySeparatorIndex + 1);
        var ciphertextSeparatorIndex = nonceSeparatorIndex < 0
            ? -1
            : envelope.IndexOf(':', nonceSeparatorIndex + 1);

        if (!envelope.StartsWith(AesGcmEnvelopePrefix + ":", StringComparison.Ordinal) ||
            keySeparatorIndex <= payloadStart ||
            nonceSeparatorIndex <= keySeparatorIndex + 1 ||
            ciphertextSeparatorIndex <= nonceSeparatorIndex + 1 ||
            ciphertextSeparatorIndex == envelope.Length - 1 ||
            envelope.IndexOf(':', ciphertextSeparatorIndex + 1) >= 0)
        {
            throw new FormatException("The authenticated encryption envelope is malformed.");
        }

        var keyId = envelope[payloadStart..keySeparatorIndex];
        if (!EncryptionOptionsValidator.IsSafeKeyId(keyId))
        {
            throw new FormatException("The authenticated encryption key identifier is invalid.");
        }

        byte[]? key = null;
        byte[]? nonce = null;
        byte[]? ciphertext = null;
        byte[]? tag = null;
        byte[]? plaintext = null;
        byte[]? aad = null;

        try
        {
            key = GetKey(keyId);
            nonce = Convert.FromBase64String(envelope[(keySeparatorIndex + 1)..nonceSeparatorIndex]);
            ciphertext = Convert.FromBase64String(envelope[(nonceSeparatorIndex + 1)..ciphertextSeparatorIndex]);
            tag = Convert.FromBase64String(envelope[(ciphertextSeparatorIndex + 1)..]);
            if (nonce.Length != AesGcmNonceSize || tag.Length != AesGcmTagSize ||
                ciphertext.Length > _options.MaxPlaintextBytes)
            {
                throw new CryptographicException("The authenticated encryption envelope has invalid bounds.");
            }

            plaintext = new byte[ciphertext.Length];
            aad = CreateAad(keyId);
            using var aes = new AesGcm(key, AesGcmTagSize);
            aes.Decrypt(nonce, ciphertext, tag, plaintext, aad);
            return StrictUtf8.GetString(plaintext);
        }
        finally
        {
            ZeroIfAllocated(key);
            ZeroIfAllocated(nonce);
            ZeroIfAllocated(ciphertext);
            ZeroIfAllocated(tag);
            ZeroIfAllocated(plaintext);
            ZeroIfAllocated(aad);
        }
    }

    private string DecryptLegacyCbc(string cipherText)
    {
        EnsureCiphertextSize(cipherText);
        byte[]? iv = null;
        byte[]? ciphertext = null;
        byte[]? key = null;
        byte[]? plaintext = null;

        try
        {
            var separatorIndex = cipherText.IndexOf(':');
            if (separatorIndex > 0)
            {
                iv = Convert.FromBase64String(cipherText[..separatorIndex]);
                ciphertext = Convert.FromBase64String(cipherText[(separatorIndex + 1)..]);
            }
            else
            {
                iv = StrictUtf8.GetBytes(_options.Iv);
                ciphertext = Convert.FromBase64String(cipherText);
            }

            key = GetLegacyKey();
            if (iv.Length != 16 || ciphertext.Length > _options.MaxPlaintextBytes + 16)
            {
                throw new CryptographicException("The legacy ciphertext has invalid bounds.");
            }

            using var aes = Aes.Create();
            aes.Key = key;
            aes.IV = iv;

            using var decryptor = aes.CreateDecryptor();
            plaintext = decryptor.TransformFinalBlock(ciphertext, 0, ciphertext.Length);
            return StrictUtf8.GetString(plaintext);
        }
        finally
        {
            ZeroIfAllocated(key);
            ZeroIfAllocated(iv);
            ZeroIfAllocated(ciphertext);
            ZeroIfAllocated(plaintext);
        }
    }

    private byte[] GetLegacyKey() => EncryptionOptionsValidator.DecodeKeyMaterial(_options.Key);

    private byte[] GetKey(string keyId)
    {
        if (_options.KeyRing.TryGetValue(keyId, out var keyMaterial))
        {
            return EncryptionOptionsValidator.DecodeKeyMaterial(keyMaterial);
        }

        if (string.Equals(keyId, _options.ActiveKeyId, StringComparison.Ordinal))
        {
            return GetLegacyKey();
        }

        throw new CryptographicException("The ciphertext key identifier is not available.");
    }

    private byte[] CreateAad(string keyId)
        => StrictUtf8.GetBytes($"{AesGcmEnvelopePrefix}|{keyId}|{_options.Purpose}");

    private void EnsurePlaintextSize(string value)
    {
        if (StrictUtf8.GetByteCount(value) > _options.MaxPlaintextBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "Plaintext exceeds the configured encryption limit.");
        }
    }

    private void EnsureCiphertextSize(string cipherText)
    {
        var maximumEncodedLength = checked((_options.MaxPlaintextBytes * 2) + 1024);
        if (cipherText.Length > maximumEncodedLength)
        {
            throw new CryptographicException("Ciphertext exceeds the configured encryption limit.");
        }
    }

    private static void ZeroIfAllocated(byte[]? buffer)
    {
        if (buffer is not null)
        {
            CryptographicOperations.ZeroMemory(buffer);
        }
    }

    private static EncryptionOptions CreateSnapshot(EncryptionOptions source)
        => new()
        {
            Key = source.Key,
            Iv = source.Iv,
            WriteMode = source.WriteMode,
            ActiveKeyId = source.ActiveKeyId,
            KeyRing = source.KeyRing is null
                ? null!
                : new Dictionary<string, string>(source.KeyRing, StringComparer.Ordinal),
            AllowLegacyDecryption = source.AllowLegacyDecryption,
            Purpose = source.Purpose,
            MaxPlaintextBytes = source.MaxPlaintextBytes
        };
}
