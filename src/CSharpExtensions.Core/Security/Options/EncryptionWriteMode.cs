namespace CSharpExtensions.Core.Security.Options;

/// <summary>
/// Selects the ciphertext format used for new encryption writes.
/// </summary>
public enum EncryptionWriteMode
{
    /// <summary>
    /// Writes the existing random-IV AES-CBC format for persisted-data compatibility.
    /// </summary>
    LegacyCbc = 0,

    /// <summary>
    /// Writes a versioned authenticated AES-GCM envelope.
    /// </summary>
    AesGcmV2 = 1
}
