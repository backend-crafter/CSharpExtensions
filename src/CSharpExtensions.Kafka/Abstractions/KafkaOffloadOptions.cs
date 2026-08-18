namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Configuration options for S3 Claim Check large payload offloading.
/// </summary>
public sealed class KafkaOffloadOptions
{
    /// <summary>
    /// Payload size threshold in bytes above which messages are offloaded to S3.
    /// Default 1 MB (1048576 bytes).
    /// </summary>
    public int InlineThresholdBytes { get; set; } = 1048576;

    /// <summary>
    /// Maximum number of bytes accepted when resolving a claim-check reference.
    /// This protects consumers from unbounded remote payload allocation.
    /// </summary>
    public int MaxDownloadBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// S3 bucket name for storing offloaded payloads.
    /// </summary>
    public string BucketName { get; set; } = "";

    /// <summary>
    /// AWS region for the S3 bucket.
    /// </summary>
    public string Region { get; set; } = "";

    /// <summary>
    /// Key prefix for S3 objects. Used for organizing offloaded payloads.
    /// </summary>
    public string KeyPrefix { get; set; } = "";

    /// <summary>
    /// Server-side encryption policy applied to uploaded claim-check objects.
    /// BucketDefault preserves the bucket's configured default encryption policy.
    /// </summary>
    public S3ServerSideEncryptionPolicy ServerSideEncryption { get; set; } =
        S3ServerSideEncryptionPolicy.BucketDefault;

    /// <summary>
    /// AWS KMS key identifier used only when ServerSideEncryption is Kms.
    /// </summary>
    public string KmsKeyId { get; set; } = "";

    /// <summary>
    /// When true, skips SHA-256 hash verification on download. Not recommended for production.
    /// </summary>
    public bool SkipHashVerification { get; set; } = false;

    /// <summary>
    /// Strategy for S3 object TTL and lifecycle management.
    /// </summary>
    public S3ExpirationStrategy ExpirationStrategy { get; set; } = S3ExpirationStrategy.PrefixPath;

    /// <summary>
    /// Tag key used by S3 Lifecycle rules when using the ObjectTagging expiration strategy.
    /// </summary>
    public string LifecycleTagName { get; set; } = "RetentionDays";

    /// <summary>
    /// Number of days before offloaded S3 objects expire.
    /// </summary>
    public int RetentionDays { get; set; } = 7;

    /// <summary>
    /// Additional custom tags applied to offloaded S3 objects.
    /// </summary>
    public Dictionary<string, string> CustomObjectTags { get; set; } = new();
}

/// <summary>
/// Server-side encryption policies supported for S3 claim-check objects.
/// </summary>
public enum S3ServerSideEncryptionPolicy
{
    BucketDefault = 0,
    Aes256 = 1,
    Kms = 2
}
