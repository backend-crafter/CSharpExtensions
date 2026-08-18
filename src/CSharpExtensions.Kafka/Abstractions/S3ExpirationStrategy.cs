namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Defines object-level TTL strategy for S3 object expiration.
/// </summary>
public enum S3ExpirationStrategy
{
    PrefixPath, // Route objects to dedicated directories (e.g., /ttl-1d/, /ttl-14d/) mapped to S3 Lifecycle rules
    ObjectTagging // Tag objects with metadata tags evaluated by S3 Lifecycle rules
}
