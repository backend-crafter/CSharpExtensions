namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Resolves secret key material used for Kafka HMAC signatures.
/// </summary>
public interface IKafkaSignatureKeyProvider
{
    /// <summary>
    /// Gets the active HMAC key without exposing it through logs or diagnostics.
    /// The returned array must be a new caller-owned copy because the caller clears it after use.
    /// </summary>
    byte[] GetKey();

    /// <summary>
    /// Gets the stable identifier of the active signing key.
    /// </summary>
    string GetActiveKeyId() => "default";

    /// <summary>
    /// Resolves key material for signature verification during key rotation.
    /// A non-null result must be a new caller-owned copy because the caller clears it after use.
    /// </summary>
    byte[]? GetVerificationKey(string keyId) =>
        string.Equals(keyId, GetActiveKeyId(), System.StringComparison.Ordinal) ? GetKey() : null;
}
