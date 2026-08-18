using System.Security.Cryptography;
using System.Text;
using CSharpExtensions.Core.Phone;
using CSharpExtensions.Core.Security.Extensions;
using CSharpExtensions.Core.Security.Helpers;
using CSharpExtensions.Core.Security.Interfaces;
using CSharpExtensions.Core.Security.Options;
using CSharpExtensions.Core.Security.Pii;
using CSharpExtensions.Core.Security.Services;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace CSharpExtensions.Tests;

public class SecurityTests
{
    private readonly IEncryptionService _encryptionService;
    private const string Key = "12345678901234567890123456789012"; // 32 bytes
    private const string Iv = "1234567890123456"; // 16 bytes

    public SecurityTests()
    {
        var options = new EncryptionOptions { Key = Key, Iv = Iv };
        var optionsMock = new Mock<IOptions<EncryptionOptions>>();
        optionsMock.Setup(x => x.Value).Returns(options);
        
        _encryptionService = new EncryptionService(optionsMock.Object);
    }

    [Fact]
    public void EncryptDecrypt_ShouldReturnOriginalValue()
    {
        // Arrange
        var original = "Sensitive Data 123";

        // Act
        var encrypted = _encryptionService.Encrypt(original);
        var decrypted = _encryptionService.Decrypt(encrypted);

        // Assert
        Assert.NotEqual(original, encrypted);
        Assert.Equal(original, decrypted);
    }

    [Fact]
    public void Encrypt_ShouldUseRandomIV_YieldingDifferentCipherTexts()
    {
        // Arrange
        var original = "Identical Data";

        // Act
        var encrypted1 = _encryptionService.Encrypt(original);
        var encrypted2 = _encryptionService.Encrypt(original);

        // Assert
        Assert.NotEqual(encrypted1, encrypted2);
        Assert.Contains(":", encrypted1);
        Assert.Contains(":", encrypted2);
        
        Assert.Equal(original, _encryptionService.Decrypt(encrypted1));
        Assert.Equal(original, _encryptionService.Decrypt(encrypted2));
    }

    [Fact]
    public void Encrypt_DefaultWriteMode_ShouldRemainLegacyCompatible()
    {
        var encrypted = _encryptionService.Encrypt("Persisted value");

        Assert.False(encrypted.StartsWith("v2:", StringComparison.Ordinal));
        Assert.Equal(2, encrypted.Split(':').Length);
    }

    [Fact]
    public void Encrypt_AesGcmV2_ShouldRoundTripWithBoundPurposeAndKeyId()
    {
        var options = new EncryptionOptions
        {
            Key = Key,
            Iv = Iv,
            WriteMode = EncryptionWriteMode.AesGcmV2,
            ActiveKeyId = "primary-2026",
            Purpose = "user-pii",
            KeyRing = new Dictionary<string, string>
            {
                ["primary-2026"] = Key
            }
        };
        var service = new EncryptionService(Options.Create(options));

        var encrypted = service.Encrypt("Authenticated value");

        Assert.StartsWith("v2:primary-2026:", encrypted, StringComparison.Ordinal);
        Assert.Equal("Authenticated value", service.Decrypt(encrypted));
    }

    [Fact]
    public void TryDecrypt_AesGcmV2TamperingOrWrongPurpose_ShouldFailClosed()
    {
        var options = new EncryptionOptions
        {
            Key = Key,
            Iv = Iv,
            WriteMode = EncryptionWriteMode.AesGcmV2,
            ActiveKeyId = "primary",
            Purpose = "correct-purpose"
        };
        var service = new EncryptionService(Options.Create(options));
        var encrypted = service.Encrypt("Authenticated value");
        var parts = encrypted.Split(':');
        parts[4] = (parts[4][0] == 'A' ? 'B' : 'A') + parts[4][1..];
        var tampered = string.Join(':', parts);

        Assert.False(service.TryDecrypt(tampered, out var tamperedPlaintext));
        Assert.Equal(string.Empty, tamperedPlaintext);
        Assert.Throws<CryptographicException>(() => service.Decrypt(tampered));

        var wrongPurposeService = new EncryptionService(Options.Create(new EncryptionOptions
        {
            Key = Key,
            Iv = Iv,
            WriteMode = EncryptionWriteMode.AesGcmV2,
            ActiveKeyId = "primary",
            Purpose = "wrong-purpose"
        }));

        Assert.False(wrongPurposeService.TryDecrypt(encrypted, out _));
    }

    [Theory]
    [InlineData("v2:primary:not-base64:AA==:AAAAAAAAAAAAAAAAAAAAAA==")]
    [InlineData("v2:primary:AAAAAAAAAAAAAAAA:not-base64:AAAAAAAAAAAAAAAAAAAAAA==")]
    [InlineData("v2:primary:AAAAAAAAAAAAAAAA:AA==:not-base64")]
    public void TryDecrypt_AesGcmV2MalformedBase64_ShouldFailClosed(string malformedEnvelope)
    {
        var service = new EncryptionService(Options.Create(new EncryptionOptions
        {
            Key = Key,
            Iv = Iv,
            WriteMode = EncryptionWriteMode.AesGcmV2,
            ActiveKeyId = "primary",
            Purpose = "user-pii"
        }));

        Assert.False(service.TryDecrypt(malformedEnvelope, out var plaintext));
        Assert.Equal(string.Empty, plaintext);
    }

    [Fact]
    public void TryDecrypt_AesGcmV2WithManySeparators_ShouldFailClosed()
    {
        var service = new EncryptionService(Options.Create(new EncryptionOptions
        {
            Key = Key,
            Iv = Iv,
            WriteMode = EncryptionWriteMode.AesGcmV2,
            ActiveKeyId = "primary",
            Purpose = "user-pii"
        }));
        var malformedEnvelope = "v2:primary:" + new string(':', 100_000);

        Assert.False(service.TryDecrypt(malformedEnvelope, out var plaintext));
        Assert.Equal(string.Empty, plaintext);
    }

    [Theory]
    [InlineData("not-base64:AA==")]
    [InlineData("AAAAAAAAAAAAAAAAAAAAAA==:not-base64")]
    public void TryDecrypt_LegacyMalformedBase64_ShouldFailClosed(string malformedCiphertext)
    {
        Assert.False(_encryptionService.TryDecrypt(malformedCiphertext, out var plaintext));
        Assert.Equal(string.Empty, plaintext);
    }

    [Fact]
    public void TryDecrypt_MalformedCiphertext_ShouldNotReturnCiphertextAsPlaintext()
    {
        Assert.False(_encryptionService.TryDecrypt("not-ciphertext", out var plaintext));
        Assert.Equal(string.Empty, plaintext);
    }

    [Fact]
    public void TryDecrypt_WhenLegacyReadsAreDisabled_ShouldAcceptOnlyAuthenticatedEnvelope()
    {
        var legacyCiphertext = _encryptionService.Encrypt("legacy-value");
        var authenticatedOnly = new EncryptionService(Options.Create(new EncryptionOptions
        {
            WriteMode = EncryptionWriteMode.AesGcmV2,
            AllowLegacyDecryption = false,
            ActiveKeyId = "primary",
            Purpose = "migration-complete",
            KeyRing = new Dictionary<string, string>
            {
                ["primary"] = Key
            }
        }));
        var authenticatedCiphertext = authenticatedOnly.Encrypt("v2-value");

        Assert.False(authenticatedOnly.TryDecrypt(legacyCiphertext, out _));
        Assert.Throws<CryptographicException>(() => authenticatedOnly.Decrypt(legacyCiphertext));
        Assert.Equal("v2-value", authenticatedOnly.Decrypt(authenticatedCiphertext));
    }

    [Fact]
    public void EncryptionOptionsValidator_ShouldRejectUnresolvedActiveKeyWhenKeyRingIsUsed()
    {
        var options = new EncryptionOptions
        {
            Key = Key,
            Iv = Iv,
            WriteMode = EncryptionWriteMode.AesGcmV2,
            ActiveKeyId = "missing",
            KeyRing = new Dictionary<string, string>
            {
                ["available"] = Key
            }
        };

        var result = new EncryptionOptionsValidator().Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(result.Failures, failure => failure.Contains("ActiveKeyId", StringComparison.Ordinal));
    }

    [Fact]
    public void EncryptionService_AuthenticatedOnlyMode_ShouldNotRequireLegacyMaterialAndShouldSnapshotOptions()
    {
        var mutableOptions = new EncryptionOptions
        {
            Key = string.Empty,
            Iv = string.Empty,
            WriteMode = EncryptionWriteMode.AesGcmV2,
            AllowLegacyDecryption = false,
            ActiveKeyId = "primary",
            Purpose = "immutable-purpose",
            KeyRing = new Dictionary<string, string>
            {
                ["primary"] = Key
            }
        };
        var writer = new EncryptionService(Options.Create(mutableOptions));

        mutableOptions.ActiveKeyId = "mutated";
        mutableOptions.Purpose = "mutated-purpose";
        mutableOptions.KeyRing.Clear();
        var ciphertext = writer.Encrypt("snapshot-value");

        var reader = new EncryptionService(Options.Create(new EncryptionOptions
        {
            WriteMode = EncryptionWriteMode.AesGcmV2,
            AllowLegacyDecryption = false,
            ActiveKeyId = "primary",
            Purpose = "immutable-purpose",
            KeyRing = new Dictionary<string, string>
            {
                ["primary"] = Key
            }
        }));
        Assert.Equal("snapshot-value", reader.Decrypt(ciphertext));
        Assert.False(reader.TryDecrypt("v2:../:AA==:AA==:AAAAAAAAAAAAAAAAAAAAAA==", out _));
    }

    [Fact]
    public void Decrypt_LegacyStaticIV_ShouldDecryptSuccessfully()
    {
        // Arrange
        var original = "Legacy PII Data";
        byte[] legacyCipherBytes;
        using (var aes = System.Security.Cryptography.Aes.Create())
        {
            aes.Key = System.Text.Encoding.UTF8.GetBytes(Key);
            aes.IV = System.Text.Encoding.UTF8.GetBytes(Iv);
            using var encryptor = aes.CreateEncryptor();
            var plaintextBytes = System.Text.Encoding.UTF8.GetBytes(original);
            legacyCipherBytes = encryptor.TransformFinalBlock(plaintextBytes, 0, plaintextBytes.Length);
        }
        var legacyEncryptedBase64 = Convert.ToBase64String(legacyCipherBytes);

        // Assert
        Assert.DoesNotContain(":", legacyEncryptedBase64);

        // Act
        var decrypted = _encryptionService.Decrypt(legacyEncryptedBase64);

        // Assert
        Assert.Equal(original, decrypted);
    }

    [Fact]
    public void MaskPhone_ShouldMaskCorrectly()
    {
        // Arrange
        var phone = "+37499123456";

        // Act
        var masked = _encryptionService.MaskPhone(phone);

        // Assert
        Assert.Equal("+374*****56", masked);
    }

    [Fact]
    public void MaskEmail_ShouldMaskCorrectly()
    {
        // Arrange
        var email = "john.doe@example.com";

        // Act
        var masked = _encryptionService.MaskEmail(email);

        // Assert
        Assert.Equal("j***e@example.com", masked);
    }

    [Fact]
    public void MaskText_ShouldMaskCorrectly()
    {
        // Arrange
        var text = "VerySecretString";

        // Act
        var masked = _encryptionService.MaskText(text, 2, 2);

        // Assert
        Assert.Equal("Ve***ng", masked);
    }

    [Fact]
    public void Masking_ShouldFailClosedForShortValuesAndInvalidVisibleBounds()
    {
        Assert.Equal("**", "+1".MaskPhone());
        Assert.Equal("****", "abcd".MaskText(2, 2));
        Assert.Throws<ArgumentOutOfRangeException>(() => "secret".MaskText(-1, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => "secret".MaskText(1, -1));
    }

    [Fact]
    public void PhoneNormalization_ShouldRejectUnboundedOrNonNumericInput()
    {
        Assert.Null(new string('1', 129).NormalizePhone());
        Assert.Null("not-a-phone".NormalizePhone());
        Assert.False(new string('1', 129).IsValidPhone());
    }

    [Fact]
    public void ComputeHash_LargeUnicodeInput_ShouldMatchFrameworkSha256()
    {
        var input = string.Concat(Enumerable.Repeat("A🙂\u0411", 10_000));
        var expected = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(input)))
            .ToLowerInvariant();

        var actual = HashGenerator.ComputeHash(input);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void HmacApis_ShouldSeparateStrongAndLegacyAlgorithms()
    {
        const string key = "0123456789abcdef0123456789abcdef";
        const string message = "payload";
        var expectedSha512 = Convert.ToHexString(
                HMACSHA512.HashData(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(message)))
            .ToLowerInvariant();
        var expectedMd5 = Convert.ToHexString(
                HMACMD5.HashData(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(message)))
            .ToLowerInvariant();

        Assert.Equal(expectedSha512, HashGenerator.ComputeStrongHmac(key, message, HashAlgorithmName.SHA512));
        Assert.Equal(expectedMd5, HashGenerator.ComputeLegacyHmac(key, message, HashAlgorithmName.MD5));
        Assert.Throws<NotSupportedException>(() =>
            HashGenerator.ComputeStrongHmac(key, message, HashAlgorithmName.MD5));
        Assert.Throws<ArgumentException>(() =>
            HashGenerator.ComputeLegacyHmac(key, message, HashAlgorithmName.SHA256));
    }

    [Fact]
    public void StrongHmac_ShouldSupportEmptyPayloadEnforceStrongKeyAndVerifyInConstantTime()
    {
        const string strongKey = "0123456789abcdef0123456789abcdef";
        var expected = Convert.ToHexString(
                HMACSHA256.HashData(Encoding.UTF8.GetBytes(strongKey), ReadOnlySpan<byte>.Empty))
            .ToLowerInvariant();

        var actual = HashGenerator.ComputeStrongHmac(strongKey, string.Empty, HashAlgorithmName.SHA256);

        Assert.Equal(expected, actual);
        Assert.True(HashGenerator.VerifyStrongHmac(strongKey, string.Empty, expected, HashAlgorithmName.SHA256));
        Assert.True(HashGenerator.VerifyStrongHmac(strongKey, string.Empty, expected.ToUpperInvariant(), HashAlgorithmName.SHA256));
        Assert.False(HashGenerator.VerifyStrongHmac(strongKey, string.Empty, new string('0', 64), HashAlgorithmName.SHA256));
        Assert.Throws<ArgumentNullException>(() =>
            HashGenerator.ComputeStrongHmac(strongKey, (string)null!, HashAlgorithmName.SHA256));
        Assert.Throws<ArgumentException>(() =>
            HashGenerator.ComputeStrongHmac("short-key", "payload", HashAlgorithmName.SHA256));

        Assert.NotEmpty(HashGenerator.ComputeHmac("short-key", "payload"));
        Assert.Equal(string.Empty, HashGenerator.ComputeHmac("short-key", string.Empty));
    }

    [Fact]
    public void ComputeHmac_ShouldRejectUnboundedKeyMaterial()
    {
        var oversizedKey = new string('k', (64 * 1024) + 1);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            HashGenerator.ComputeHmac(oversizedKey, "payload"));
    }

    [Fact]
    public void GenerateAlphanumeric_ShouldHaveCorrectLengthAndAlphanumericCharsOnly()
    {
        // Arrange
        var length = 48;

        // Act
        var secret = SecretGenerator.GenerateAlphanumeric(length);

        // Assert
        Assert.Equal(length, secret.Length);
        Assert.All(secret, c => Assert.True(char.IsLetterOrDigit(c)));
    }

    [Fact]
    public void GenerateComplex_ShouldHaveCorrectLength()
    {
        // Arrange
        var length = 64;

        // Act
        var secret = SecretGenerator.GenerateComplex(length);

        // Assert
        Assert.Equal(length, secret.Length);
    }

    [Fact]
    public void GenerateWithHash_ShouldReturnValidSecretAndCorrectSha256Hash()
    {
        // Arrange
        var length = 32;

        // Act
        var (secret, hash) = SecretGenerator.GenerateWithHash(length);

        // Assert
        Assert.Equal(length, secret.Length);
        var expectedHash = HashGenerator.ComputeHash(secret);
        Assert.Equal(expectedHash, hash);
    }

    [Theory]
    [InlineData(30)]
    [InlineData(31)]
    [InlineData(257)]
    [InlineData(500)]
    public void SecretGenerator_ShouldThrowWhenLengthIsOutOfRange(int invalidLength)
    {
        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => SecretGenerator.GenerateAlphanumeric(invalidLength));
        Assert.Throws<ArgumentOutOfRangeException>(() => SecretGenerator.GenerateComplex(invalidLength));
        Assert.Throws<ArgumentOutOfRangeException>(() => SecretGenerator.GenerateWithHash(invalidLength));
    }

    [Fact]
    public void OtpHelper_GenerateNumeric_ShouldGenerateValidNumericString()
    {
        // Act
        var code = OtpHelper.GenerateNumeric(6);

        // Assert
        Assert.Equal(6, code.Length);
        Assert.All(code, c => Assert.True(char.IsDigit(c)));
    }

    [Fact]
    public void OtpHelper_BeginOverride_ShouldRestorePreviousGenerator()
    {
        // Arrange
        var mockGenerator = new Mock<IOtpGenerator>();
        mockGenerator.Setup(g => g.GenerateNumeric(6)).Returns("999999");

        // Act
        string code;
        using (OtpHelper.BeginOverride(mockGenerator.Object))
        {
            code = OtpHelper.GenerateNumeric(6);
        }

        var codeAfterReset = OtpHelper.GenerateNumeric(6);

        // Assert
        Assert.Equal("999999", code);
        Assert.NotEqual("999999", codeAfterReset);
    }

    [Fact]
    public async Task OtpHelper_Overrides_ShouldBeIsolatedAcrossExecutionContexts()
    {
        static async Task<string> GenerateAsync(string expected)
        {
            var generator = new Mock<IOtpGenerator>();
            generator.Setup(instance => instance.GenerateNumeric(6)).Returns(expected);
            using var scope = OtpHelper.BeginOverride(generator.Object);
            await Task.Yield();
            return OtpHelper.GenerateNumeric(6);
        }

        var values = await Task.WhenAll(
            Task.Run(() => GenerateAsync("111111")),
            Task.Run(() => GenerateAsync("999999")));

        Assert.Equal(["111111", "999999"], values);
    }
}

