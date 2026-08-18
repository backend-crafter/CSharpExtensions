namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Configuration options for the Redis-backed idempotency (deduplication) feature.
/// </summary>
public sealed class IdempotencyOptions
{
    /// <summary>
    /// Redis connection alias used for the idempotency provider.
    /// Must match an alias registered via <c>KafkaBuilder.AddRedisConnection()</c>.
    /// Defaults to "Default".
    /// </summary>
    public string RedisConnectionAlias { get; set; } = "Default";
}
