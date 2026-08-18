using System.Security.Cryptography;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.Kafka.Core;

/// <summary>
/// Validates HMAC key material through the active key provider during application startup.
/// </summary>
internal sealed class KafkaSignatureKeyValidationHostedService(
    IOptions<KafkaOptions> options,
    IKafkaSignatureKeyProvider keyProvider) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (options.Value.Security.SignatureWriteVersion != KafkaSignatureWriteVersion.HmacSha256V2)
        {
            return Task.CompletedTask;
        }

        var keyId = keyProvider.GetActiveKeyId();
        SignatureService.ValidateKeyId(keyId);
        byte[]? key = null;
        byte[]? verificationKey = null;
        try
        {
            key = keyProvider.GetKey();
            if (key is null || key.Length < 32)
            {
                throw new InvalidOperationException("Kafka HMAC signature key must contain at least 32 bytes.");
            }

            verificationKey = keyProvider.GetVerificationKey(keyId);
            if (verificationKey is null || verificationKey.Length < 32)
            {
                throw new InvalidOperationException("Kafka HMAC active key is not available through the verification key ring.");
            }
        }
        finally
        {
            if (key is not null)
            {
                CryptographicOperations.ZeroMemory(key);
            }

            if (verificationKey is not null)
            {
                CryptographicOperations.ZeroMemory(verificationKey);
            }
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
