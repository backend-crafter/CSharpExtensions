namespace CSharpExtensions.Foundation.Security.Options;

/// <summary>
/// Configuration options for the encryption service.
/// </summary>
public class EncryptionOptions
{
    /// <summary>
    /// AES Key (32 characters for 256-bit encryption).
    /// </summary>
    public string Key { get; set; } = string.Empty;

    /// <summary>
    /// AES Initialization Vector (16 characters).
    /// </summary>
    public string Iv { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the format used for new writes. Legacy CBC remains the default during migration.
    /// </summary>
    public EncryptionWriteMode WriteMode { get; set; } = EncryptionWriteMode.LegacyCbc;

    /// <summary>
    /// Gets or sets the identifier embedded in new AES-GCM envelopes. When <see cref="KeyRing"/> is empty,
    /// the existing <see cref="Key"/> is the active-key fallback for a gradual migration.
    /// </summary>
    public string ActiveKeyId { get; set; } = "default";

    /// <summary>
    /// Gets or sets decryption keys by key identifier. Values may be UTF-8 text or use a <c>base64:</c> prefix.
    /// </summary>
    public Dictionary<string, string> KeyRing { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Gets or sets whether unauthenticated legacy CBC ciphertext can still be read during migration.
    /// Disable after all persisted values have been rewritten as authenticated v2 envelopes.
    /// </summary>
    public bool AllowLegacyDecryption { get; set; } = true;

    /// <summary>
    /// Gets or sets the purpose bound to authenticated encryption as additional authenticated data.
    /// </summary>
    public string Purpose { get; set; } = "CSharpExtensions.Security";

    /// <summary>
    /// Gets or sets the maximum UTF-8 plaintext size accepted by the service.
    /// </summary>
    public int MaxPlaintextBytes { get; set; } = 1024 * 1024;
}
