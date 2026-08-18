using System.Text;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.Kafka.Core;

internal sealed class ConfigurationKafkaSignatureKeyProvider(
    IConfiguration configuration,
    IOptions<KafkaOptions> options) : IKafkaSignatureKeyProvider
{
    public byte[] GetKey()
    {
        var path = options.Value.Security.SignatureKeyConfigurationPath;
        var value = configuration[path];
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException("Kafka HMAC signature key is not configured.");
        }

        return Encoding.UTF8.GetBytes(value);
    }

    public string GetActiveKeyId() => options.Value.Security.SignatureKeyId;

    public byte[]? GetVerificationKey(string keyId)
    {
        if (string.Equals(keyId, GetActiveKeyId(), StringComparison.Ordinal))
        {
            return GetKey();
        }

        if (!options.Value.Security.VerificationKeyConfigurationPaths.TryGetValue(keyId, out var path)
            || string.IsNullOrWhiteSpace(path))
        {
            return null;
        }

        var value = configuration[path];
        return string.IsNullOrWhiteSpace(value) ? null : Encoding.UTF8.GetBytes(value);
    }
}
