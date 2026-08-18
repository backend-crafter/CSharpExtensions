using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using CSharpExtensions.Core.Helpers;
using CSharpExtensions.Core.Json;
using CSharpExtensions.Core.Railway;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.Logging;

namespace CSharpExtensions.Kafka.Core;

/// <summary>
/// Handles offloading and downloading of large message payloads to and from AWS S3.
/// </summary>
public sealed class S3ClaimCheckOffloader
{
    private const int MaximumSchemaNameCharacters = 256;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private readonly IAmazonS3? _s3Client;
    private readonly ILogger<S3ClaimCheckOffloader>? _logger;

    public S3ClaimCheckOffloader(
        IAmazonS3? s3Client = null,
        ILogger<S3ClaimCheckOffloader>? logger = null)
    {
        _s3Client = s3Client;
        _logger = logger;
    }

    /// <summary>
    /// Uploads a large payload to S3 and returns a compact reference JSON.
    /// </summary>
    public async Task<Result<string>> OffloadAsync(
        string payloadJson,
        string schemaName,
        KafkaOffloadOptions options,
        CancellationToken cancellationToken)
    {
        if (payloadJson is null) throw new ArgumentNullException(nameof(payloadJson));
        if (options is null) throw new ArgumentNullException(nameof(options));

        cancellationToken.ThrowIfCancellationRequested();

        if (!BoundedIdentifier.TryNormalize(
                schemaName,
                out var normalizedSchemaName,
                MaximumSchemaNameCharacters))
        {
            return Result.Failure<string>("Kafka claim-check schema name is invalid.");
        }

        long byteCount;
        try
        {
            byteCount = StrictUtf8.GetByteCount(payloadJson);
        }
        catch (EncoderFallbackException)
        {
            return Result.Failure<string>("Kafka claim-check payload is not valid UTF-8 text.");
        }

        if (byteCount <= 0 || options.MaxDownloadBytes <= 0 || byteCount > options.MaxDownloadBytes)
        {
            return Result.Failure<string>("Payload size is outside the configured claim-check limit.");
        }

        if (_s3Client is null)
        {
            _logger?.LogWarning("AWS S3 client is not configured. Cannot offload the Kafka claim-check payload.");
            return Result.Failure<string>("AWS S3 Client is not configured. Cannot offload large payload.");
        }

        byte[]? payloadBytes = null;
        byte[]? hashBytes = null;
        try
        {
            payloadBytes = StrictUtf8.GetBytes(payloadJson);

            hashBytes = SHA256.HashData(payloadBytes);

            var sha256Hash = Convert.ToHexString(hashBytes).ToLowerInvariant();

            // Build S3 Object Key based on expiration strategy
            var objectKey = BuildObjectKey(options, sha256Hash);

            var bucketName = options.BucketName ?? throw new InvalidOperationException("AWS S3 bucket name is not configured.");

            using var payloadStream = new MemoryStream(payloadBytes, writable: false);
            var putRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey,
                InputStream = payloadStream,
                ContentType = "application/json"
            };
            ApplyServerSideEncryption(putRequest, options);

            // Inject lifecycle expiration tag when ObjectTagging strategy is used
            if (options.ExpirationStrategy == S3ExpirationStrategy.ObjectTagging)
            {
                putRequest.TagSet ??= new List<Tag>();
                putRequest.TagSet.Add(new Tag
                {
                    Key = options.LifecycleTagName,
                    Value = options.RetentionDays.ToString(CultureInfo.InvariantCulture)
                });
            }

            // Always inject custom object tags (ServiceName, Environment, etc.) regardless of expiration strategy
            if (options.CustomObjectTags is not null && options.CustomObjectTags.Count > 0)
            {
                putRequest.TagSet ??= new List<Tag>();
                foreach (var tag in options.CustomObjectTags)
                {
                    putRequest.TagSet.Add(new Tag { Key = tag.Key, Value = tag.Value });
                }
            }

            await _s3Client.PutObjectAsync(putRequest, cancellationToken);

            // Construct compact reference envelope
            var envelope = new Dictionary<string, object>
            {
                ["$ref"] = true,
                ["schema"] = normalizedSchemaName,
                ["byteCount"] = byteCount,
                ["sha256"] = sha256Hash,
                ["s3"] = new Dictionary<string, string>
                {
                    ["bucket"] = bucketName,
                    ["key"] = objectKey,
                    ["region"] = options.Region ?? string.Empty
                }
            };

            var compactMessage = JsonSerializer.Serialize(envelope, JsonOptions.Default);
            return Result.Success(compactMessage);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger?.LogError("Failed to offload Kafka claim-check payload. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure<string>("Failed to offload payload to AWS S3.");
        }
        finally
        {
            ZeroMemory(payloadBytes);
            ZeroMemory(hashBytes);
        }
    }

    /// <summary>
    /// Downloads the payload from S3 and verifies its integrity.
    /// </summary>
    public async Task<Result<string>> DownloadAsync(
        JsonElement referenceEnvelope,
        KafkaOffloadOptions options,
        CancellationToken cancellationToken)
    {
        if (options is null) throw new ArgumentNullException(nameof(options));

        cancellationToken.ThrowIfCancellationRequested();

        if (_s3Client is null)
        {
            _logger?.LogWarning("AWS S3 client is not configured. Cannot download offloaded payload reference.");
            return Result.Failure<string>("AWS S3 Client is not configured. Cannot download offloaded payload.");
        }

        byte[]? readBuffer = null;
        byte[]? expectedHashBytes = null;
        byte[]? actualHashBytes = null;
        MemoryStream? payloadBuffer = null;
        try
        {
            if (options.SkipHashVerification)
            {
                return Result.Failure<string>("S3 claim-check integrity verification cannot be disabled.");
            }

            if (!referenceEnvelope.TryGetProperty("s3", out var s3Property)
                || s3Property.ValueKind != JsonValueKind.Object)
            {
                return Result.Failure<string>("Invalid reference envelope: missing 's3' property.");
            }

            if (!TryGetRequiredString(s3Property, "bucket", out var bucketName)
                || !TryGetRequiredString(s3Property, "key", out var objectKey)
                || !TryGetRequiredString(referenceEnvelope, "sha256", out var expectedHash)
                || !referenceEnvelope.TryGetProperty("byteCount", out var byteCountProperty)
                || !byteCountProperty.TryGetInt64(out var expectedByteCount))
            {
                return Result.Failure<string>("Invalid reference envelope: required S3 integrity fields are missing.");
            }

            if (!string.Equals(bucketName, options.BucketName, StringComparison.Ordinal))
            {
                return Result.Failure<string>("Invalid reference envelope: bucket does not match the configured claim-check bucket.");
            }

            if (s3Property.TryGetProperty("region", out var regionProperty)
                && regionProperty.ValueKind == JsonValueKind.String
                && !string.Equals(regionProperty.GetString(), options.Region, StringComparison.Ordinal))
            {
                return Result.Failure<string>("Invalid reference envelope: region does not match the configured claim-check region.");
            }

            if (expectedByteCount <= 0 || options.MaxDownloadBytes <= 0 || expectedByteCount > options.MaxDownloadBytes)
            {
                return Result.Failure<string>("Invalid reference envelope: payload size is outside the configured claim-check limit.");
            }

            if (!IsSha256Hex(expectedHash))
            {
                return Result.Failure<string>("Invalid reference envelope: SHA-256 digest is missing or malformed.");
            }

            var normalizedHash = expectedHash.ToLowerInvariant();
            var expectedObjectKey = BuildObjectKey(options, normalizedHash);
            if (!string.Equals(objectKey, expectedObjectKey, StringComparison.Ordinal))
            {
                return Result.Failure<string>("Invalid reference envelope: object key is outside the configured claim-check prefix.");
            }

            var getRequest = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            };

            using var response = await _s3Client.GetObjectAsync(getRequest, cancellationToken);
            if (response.ContentLength > options.MaxDownloadBytes
                || (response.ContentLength >= 0 && response.ContentLength != expectedByteCount))
            {
                return Result.Failure<string>("Integrity check failed: S3 content length does not match the signed reference metadata.");
            }

            payloadBuffer = new MemoryStream((int)expectedByteCount);
            using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            readBuffer = new byte[81920];
            long actualByteCount = 0;
            while (true)
            {
                var read = await response.ResponseStream.ReadAsync(readBuffer.AsMemory(0, readBuffer.Length), cancellationToken);
                if (read == 0)
                {
                    break;
                }

                actualByteCount += read;
                if (actualByteCount > expectedByteCount || actualByteCount > options.MaxDownloadBytes)
                {
                    return Result.Failure<string>("Integrity check failed: S3 payload exceeded its declared size.");
                }

                hasher.AppendData(readBuffer, 0, read);
                await payloadBuffer.WriteAsync(readBuffer.AsMemory(0, read), cancellationToken);
            }

            if (actualByteCount != expectedByteCount)
            {
                return Result.Failure<string>($"Integrity check failed: byte count mismatch. Expected: {expectedByteCount}, Actual: {actualByteCount}");
            }

            expectedHashBytes = Convert.FromHexString(normalizedHash);
            actualHashBytes = hasher.GetHashAndReset();
            if (expectedHashBytes.Length != actualHashBytes.Length
                || !CryptographicOperations.FixedTimeEquals(expectedHashBytes, actualHashBytes))
            {
                return Result.Failure<string>("Integrity check failed: SHA-256 hash mismatch.");
            }

            try
            {
                return Result.Success(new UTF8Encoding(false, true).GetString(payloadBuffer.GetBuffer(), 0, checked((int)actualByteCount)));
            }
            catch (DecoderFallbackException)
            {
                return Result.Failure<string>("Integrity check failed: S3 payload is not valid UTF-8.");
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger?.LogError("Failed to download Kafka claim-check payload. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure<string>("Failed to download payload from S3.");
        }
        finally
        {
            ZeroMemory(readBuffer);
            ZeroMemory(expectedHashBytes);
            ZeroMemory(actualHashBytes);
            if (payloadBuffer is not null)
            {
                if (payloadBuffer.TryGetBuffer(out var segment) && segment.Array is not null)
                {
                    CryptographicOperations.ZeroMemory(segment.Array.AsSpan(segment.Offset, segment.Count));
                }

                payloadBuffer.Dispose();
            }
        }
    }

    private static bool TryGetRequiredString(JsonElement element, string propertyName, out string value)
    {
        value = string.Empty;
        if (!element.TryGetProperty(propertyName, out var property) || property.ValueKind != JsonValueKind.String)
        {
            return false;
        }

        value = property.GetString() ?? string.Empty;
        return !string.IsNullOrWhiteSpace(value);
    }

    private static bool IsSha256Hex(string value)
    {
        if (value.Length != 64)
        {
            return false;
        }

        foreach (var character in value)
        {
            if (!Uri.IsHexDigit(character))
            {
                return false;
            }
        }

        return true;
    }

    private static string BuildObjectKey(KafkaOffloadOptions options, string sha256Hash)
    {
        var configuredPrefix = options.KeyPrefix?.Trim().Trim('/') ?? string.Empty;
        var keyPrefix = configuredPrefix.Length == 0 ? string.Empty : $"{configuredPrefix}/";
        return options.ExpirationStrategy == S3ExpirationStrategy.PrefixPath
            ? $"ttl-{options.RetentionDays}d/{keyPrefix}{sha256Hash}.json"
            : $"{keyPrefix}{sha256Hash}.json";
    }

    private static void ApplyServerSideEncryption(
        PutObjectRequest request,
        KafkaOffloadOptions options)
    {
        switch (options.ServerSideEncryption)
        {
            case S3ServerSideEncryptionPolicy.BucketDefault:
                return;
            case S3ServerSideEncryptionPolicy.Aes256:
                request.ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256;
                return;
            case S3ServerSideEncryptionPolicy.Kms:
                request.ServerSideEncryptionMethod = ServerSideEncryptionMethod.AWSKMS;
                request.ServerSideEncryptionKeyManagementServiceKeyId = options.KmsKeyId;
                return;
            default:
                throw new InvalidOperationException("Unsupported S3 server-side encryption policy.");
        }
    }

    private static void ZeroMemory(byte[]? value)
    {
        if (value is not null)
        {
            CryptographicOperations.ZeroMemory(value);
        }
    }
}
