namespace CSharpExtensions.Kafka.Abstractions;

using System.Collections.Generic;

/// <summary>
/// Message authentication migration settings.
/// </summary>
public sealed class KafkaSecuritySettings
{
    /// <summary>
    /// Signature format used for newly published messages.
    /// </summary>
    public KafkaSignatureWriteVersion SignatureWriteVersion { get; set; } = KafkaSignatureWriteVersion.LegacyV1;

    /// <summary>
    /// Allows verification of unversioned legacy encrypted digest signatures during migration.
    /// </summary>
    public bool AllowLegacyV1Verification { get; set; } = true;

    /// <summary>
    /// Configuration path resolved by the default HMAC key provider.
    /// Supply the value through a secret-backed configuration source.
    /// </summary>
    public string SignatureKeyConfigurationPath { get; set; } = "Kafka:Security:SignatureKey";

    /// <summary>
    /// Stable identifier emitted with newly generated HMAC signatures.
    /// </summary>
    public string SignatureKeyId { get; set; } = "default";

    /// <summary>
    /// Optional verification key ring mapping historical key identifiers to secret-backed configuration paths.
    /// </summary>
    public Dictionary<string, string> VerificationKeyConfigurationPaths { get; set; } = new();
}

/// <summary>
/// Supported signature formats for newly published messages.
/// </summary>
public enum KafkaSignatureWriteVersion
{
    LegacyV1 = 1,
    HmacSha256V2 = 2
}
