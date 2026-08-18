using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpExtensions.Core.Security.Interfaces;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.Kafka.Core;

/// <summary>
/// Service responsible for signing and verifying messages to prevent unauthorized injection.
/// </summary>
public sealed class SignatureService
{
    private const int SignatureMacBase64UrlLength = 43;
    private const int MaxV2SignatureLength = 112;
    private const int MaxLegacySignatureLength = 4096;
    private const int HashBufferSize = 4096;
    private static readonly byte[] ColonSeparator = [(byte)':'];
    private static readonly byte[] NewLineSeparator = [(byte)'\n'];

    private readonly IEncryptionService? _encryptionService;
    private readonly KafkaSecuritySettings _settings;
    private readonly IKafkaSignatureKeyProvider? _keyProvider;

    public SignatureService(IEncryptionService encryptionService)
        : this(encryptionService, Options.Create(new KafkaOptions()), null)
    {
    }

    [ActivatorUtilitiesConstructor]
    public SignatureService(
        IEncryptionService? encryptionService,
        IOptions<KafkaOptions> options,
        IKafkaSignatureKeyProvider? keyProvider)
    {
        _encryptionService = encryptionService;
        _settings = options?.Value.Security ?? throw new ArgumentNullException(nameof(options));
        _keyProvider = keyProvider;
    }

    /// <summary>
    /// Generates a cryptographic signature for a message payload.
    /// </summary>
    /// <param name="payloadJson">The raw payload JSON.</param>
    /// <param name="messageId">The unique message identifier.</param>
    /// <param name="correlationId">The transaction correlation identifier.</param>
    /// <returns>A base64 encoded signature string.</returns>
    public string SignMessage(string payloadJson, string messageId, string correlationId)
    {
        if (payloadJson is null) throw new ArgumentNullException(nameof(payloadJson));
        if (messageId is null) throw new ArgumentNullException(nameof(messageId));
        if (correlationId is null) throw new ArgumentNullException(nameof(correlationId));

        if (_settings.SignatureWriteVersion == KafkaSignatureWriteVersion.HmacSha256V2)
        {
            throw new InvalidOperationException(
                "Kafka HMAC v2 signing requires topic, message key, schema, and envelope context.");
        }

        return SignLegacyV1(payloadJson, messageId, correlationId);
    }

    /// <summary>
    /// Generates a signature bound to the complete Kafka transport identity.
    /// </summary>
    public string SignMessage(
        string payloadJson,
        string messageId,
        string correlationId,
        string topicName,
        string? messageKey,
        string schemaVersionKey,
        string envelopeKind)
    {
        ValidateCanonicalContext(payloadJson, messageId, correlationId, topicName, schemaVersionKey, envelopeKind);
        return _settings.SignatureWriteVersion == KafkaSignatureWriteVersion.HmacSha256V2
            ? SignV2(payloadJson, messageId, correlationId, topicName, messageKey, schemaVersionKey, envelopeKind)
            : SignLegacyV1(payloadJson, messageId, correlationId);
    }

    private string SignLegacyV1(string payloadJson, string messageId, string correlationId)
    {
        var encryptionService = _encryptionService ?? throw new InvalidOperationException(
            "Kafka LegacyV1 authentication requires an explicitly registered IEncryptionService.");

        byte[]? hashBytes = null;
        try
        {
            hashBytes = ComputeLegacyHash(payloadJson, messageId, correlationId);
            var hashBase64 = Convert.ToBase64String(hashBytes);
            return encryptionService.Encrypt(hashBase64);
        }
        finally
        {
            if (hashBytes is not null)
            {
                CryptographicOperations.ZeroMemory(hashBytes);
            }
        }
    }

    /// <summary>
    /// Verifies if the signature matches the message payload.
    /// </summary>
    /// <param name="payloadJson">The raw payload JSON.</param>
    /// <param name="messageId">The unique message identifier.</param>
    /// <param name="correlationId">The transaction correlation identifier.</param>
    /// <param name="signature">The signature to verify.</param>
    /// <returns>True if the signature is valid, false otherwise.</returns>
    public bool VerifySignature(string payloadJson, string messageId, string correlationId, string signature)
    {
        if (payloadJson is null) throw new ArgumentNullException(nameof(payloadJson));
        if (messageId is null) throw new ArgumentNullException(nameof(messageId));
        if (correlationId is null) throw new ArgumentNullException(nameof(correlationId));
        if (string.IsNullOrWhiteSpace(signature) || signature.Length > MaxLegacySignatureLength) return false;

        try
        {
            if (signature.StartsWith("v2.", StringComparison.Ordinal))
            {
                return false;
            }

            if (!_settings.AllowLegacyV1Verification)
            {
                return false;
            }

            var encryptionService = _encryptionService;
            if (encryptionService is null)
            {
                return false;
            }

            var expectedHashBase64 = encryptionService.Decrypt(signature);
            byte[]? expectedHashBytes = null;
            byte[]? actualHashBytes = null;
            try
            {
                expectedHashBytes = Convert.FromBase64String(expectedHashBase64);
                actualHashBytes = ComputeLegacyHash(payloadJson, messageId, correlationId);
                return expectedHashBytes.Length == actualHashBytes.Length
                    && CryptographicOperations.FixedTimeEquals(expectedHashBytes, actualHashBytes);
            }
            finally
            {
                if (expectedHashBytes is not null)
                {
                    CryptographicOperations.ZeroMemory(expectedHashBytes);
                }

                if (actualHashBytes is not null)
                {
                    CryptographicOperations.ZeroMemory(actualHashBytes);
                }
            }
        }
        catch
        {
            // Any decryption or parsing failure results in signature invalidation
            return false;
        }
    }

    /// <summary>
    /// Verifies a signature against the complete Kafka transport identity.
    /// </summary>
    public bool VerifySignature(
        string payloadJson,
        string messageId,
        string correlationId,
        string topicName,
        string? messageKey,
        string schemaVersionKey,
        string envelopeKind,
        string signature)
    {
        if (string.IsNullOrWhiteSpace(signature) || signature.Length > MaxLegacySignatureLength)
        {
            return false;
        }

        try
        {
            ValidateCanonicalContext(payloadJson, messageId, correlationId, topicName, schemaVersionKey, envelopeKind);
            if (signature.StartsWith("v2.", StringComparison.Ordinal))
            {
                return VerifyV2(
                    payloadJson,
                    messageId,
                    correlationId,
                    topicName,
                    messageKey,
                    schemaVersionKey,
                    envelopeKind,
                    signature);
            }

            return VerifySignature(payloadJson, messageId, correlationId, signature);
        }
        catch
        {
            return false;
        }
    }

    private string SignV2(
        string payloadJson,
        string messageId,
        string correlationId,
        string topicName,
        string? messageKey,
        string schemaVersionKey,
        string envelopeKind)
    {
        var keyId = _keyProvider?.GetActiveKeyId()
            ?? throw new InvalidOperationException("Kafka HMAC signature key provider is not registered.");
        ValidateKeyId(keyId);
        byte[]? key = null;
        byte[]? mac = null;
        try
        {
            key = _keyProvider.GetKey();
            ValidateKey(key);
            mac = ComputeV2Mac(
                key,
                keyId,
                payloadJson,
                messageId,
                correlationId,
                topicName,
                messageKey,
                schemaVersionKey,
                envelopeKind);
            return $"v2.{keyId}.{Convert.ToBase64String(mac).TrimEnd('=').Replace('+', '-').Replace('/', '_')}";
        }
        finally
        {
            ZeroMemory(key);
            ZeroMemory(mac);
        }
    }

    private bool VerifyV2(
        string payloadJson,
        string messageId,
        string correlationId,
        string topicName,
        string? messageKey,
        string schemaVersionKey,
        string envelopeKind,
        string signature)
    {
        byte[]? key = null;
        byte[]? expectedMac = null;
        try
        {
            if (signature.Length > MaxV2SignatureLength
                || !signature.StartsWith("v2.", StringComparison.Ordinal))
            {
                return false;
            }

            var keySeparatorIndex = signature.IndexOf('.', 3);
            if (keySeparatorIndex <= 3
                || signature.IndexOf('.', keySeparatorIndex + 1) >= 0)
            {
                return false;
            }

            var keyId = signature.Substring(3, keySeparatorIndex - 3);
            ValidateKeyId(keyId);
            var macSegment = signature.AsSpan(keySeparatorIndex + 1);
            if (macSegment.Length != SignatureMacBase64UrlLength)
            {
                return false;
            }

            Span<char> canonicalBase64 = stackalloc char[44];
            for (var index = 0; index < macSegment.Length; index++)
            {
                var character = macSegment[index];
                if (!(char.IsAsciiLetterOrDigit(character) || character is '-' or '_'))
                {
                    return false;
                }

                canonicalBase64[index] = character switch
                {
                    '-' => '+',
                    '_' => '/',
                    _ => character
                };
            }
            canonicalBase64[^1] = '=';

            Span<byte> suppliedMac = stackalloc byte[32];
            if (!Convert.TryFromBase64Chars(canonicalBase64, suppliedMac, out var bytesWritten)
                || bytesWritten != suppliedMac.Length)
            {
                return false;
            }

            key = _keyProvider?.GetVerificationKey(keyId);
            ValidateKey(key);
            expectedMac = ComputeV2Mac(
                key!,
                keyId,
                payloadJson,
                messageId,
                correlationId,
                topicName,
                messageKey,
                schemaVersionKey,
                envelopeKind);
            return CryptographicOperations.FixedTimeEquals(suppliedMac, expectedMac);
        }
        catch
        {
            return false;
        }
        finally
        {
            ZeroMemory(key);
            ZeroMemory(expectedMac);
        }
    }

    private static void ValidateKey(byte[]? key)
    {
        if (key is null)
        {
            throw new InvalidOperationException("Kafka HMAC verification key is not available.");
        }
        if (key.Length < 32)
        {
            throw new InvalidOperationException("Kafka HMAC signature key must contain at least 32 bytes.");
        }

    }

    private static void ZeroMemory(byte[]? value)
    {
        if (value is not null)
        {
            CryptographicOperations.ZeroMemory(value);
        }
    }

    private static byte[] ComputeLegacyHash(
        string payloadJson,
        string messageId,
        string correlationId)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = ArrayPool<byte>.Shared.Rent(HashBufferSize);
        try
        {
            AppendUtf8(hash, payloadJson, buffer);
            hash.AppendData(ColonSeparator);
            AppendUtf8(hash, messageId, buffer);
            hash.AppendData(ColonSeparator);
            AppendUtf8(hash, correlationId, buffer);
            return hash.GetHashAndReset();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(buffer);
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static byte[] ComputeV2Mac(
        byte[] key,
        string keyId,
        string payloadJson,
        string messageId,
        string correlationId,
        string topicName,
        string? messageKey,
        string schemaVersionKey,
        string envelopeKind)
    {
        using var hash = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, key);
        var buffer = ArrayPool<byte>.Shared.Rent(HashBufferSize);
        try
        {
            AppendCanonicalField(hash, "version", "2", buffer);
            AppendCanonicalField(hash, "keyId", keyId, buffer);
            AppendCanonicalField(hash, "payload", payloadJson, buffer);
            AppendCanonicalField(hash, "messageId", messageId, buffer);
            AppendCanonicalField(hash, "correlationId", correlationId, buffer);
            AppendCanonicalField(hash, "topic", topicName, buffer);
            AppendCanonicalField(hash, "messageKeyPresent", messageKey is null ? "0" : "1", buffer);
            AppendCanonicalField(hash, "messageKey", messageKey ?? string.Empty, buffer);
            AppendCanonicalField(hash, "schema", schemaVersionKey, buffer);
            AppendCanonicalField(hash, "envelope", envelopeKind, buffer);
            return hash.GetHashAndReset();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(buffer);
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static void AppendCanonicalField(
        IncrementalHash hash,
        string name,
        string value,
        byte[] buffer)
    {
        var prefix = string.Concat(
            name,
            "=",
            Encoding.UTF8.GetByteCount(value).ToString(CultureInfo.InvariantCulture),
            ":");
        AppendUtf8(hash, prefix, buffer);
        AppendUtf8(hash, value, buffer);
        hash.AppendData(NewLineSeparator);
    }

    private static void AppendUtf8(IncrementalHash hash, string value, byte[] buffer)
    {
        if (value.Length == 0)
        {
            return;
        }

        var encoder = Encoding.UTF8.GetEncoder();
        var remaining = value.AsSpan();
        while (!remaining.IsEmpty)
        {
            encoder.Convert(
                remaining,
                buffer.AsSpan(),
                flush: true,
                out var charsUsed,
                out var bytesUsed,
                out _);
            if (charsUsed == 0)
            {
                throw new InvalidOperationException("UTF-8 canonicalization made no progress.");
            }

            hash.AppendData(buffer.AsSpan(0, bytesUsed));
            remaining = remaining[charsUsed..];
        }
    }

    private static void ValidateCanonicalContext(
        string payloadJson,
        string messageId,
        string correlationId,
        string topicName,
        string schemaVersionKey,
        string envelopeKind)
    {
        ArgumentNullException.ThrowIfNull(payloadJson);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentException.ThrowIfNullOrWhiteSpace(schemaVersionKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(envelopeKind);
    }

    internal static void ValidateKeyId(string keyId)
    {
        if (string.IsNullOrWhiteSpace(keyId) || keyId.Length > 64)
        {
            throw new InvalidOperationException("Kafka HMAC signature key identifier is invalid.");
        }

        foreach (var character in keyId)
        {
            if (!(char.IsAsciiLetterOrDigit(character) || character is '-' or '_'))
            {
                throw new InvalidOperationException("Kafka HMAC signature key identifier is invalid.");
            }
        }
    }
}

internal static class KafkaEnvelopeKinds
{
    public const string Inline = "inline";
    public const string S3Reference = "s3-reference";
    public const string DeadLetter = "dead-letter";
}
