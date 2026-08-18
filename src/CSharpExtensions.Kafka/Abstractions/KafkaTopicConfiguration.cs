namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Maps physical properties, permissions, and feature toggles for a specific event topic.
/// All additional features (idempotency, authentication, offloading, DLQ) are disabled by default.
/// </summary>
public sealed class KafkaTopicConfiguration
{
    /// <summary>
    /// Cluster alias from <see cref="KafkaOptions.Clusters"/>. Empty string falls back to <see cref="KafkaOptions.DefaultClusterAlias"/>.
    /// </summary>
    public string Cluster { get; set; } = "";

    /// <summary>
    /// Physical Kafka topic name on the broker.
    /// </summary>
    public string TopicName { get; set; } = "";

    /// <summary>
    /// Consumer group identifier for this topic's subscriptions.
    /// </summary>
    public string GroupId { get; set; } = "";

    /// <summary>
    /// Access control for this topic configuration.
    /// </summary>
    public TopicPermission Permission { get; set; } = TopicPermission.ReadWrite;

    /// <summary>
    /// Topic-level SASL username. Overrides cluster-level credentials when specified.
    /// </summary>
    public string Username { get; set; } = "";

    /// <summary>
    /// Topic-level SASL password. Overrides cluster-level credentials when specified.
    /// </summary>
    public string Password { get; set; } = "";

    /// <summary>
    /// Enables the transactional outbox pattern for this topic.
    /// </summary>
    public bool UseOutbox { get; set; } = false;

    /// <summary>
    /// Enables Redis-backed idempotent consumption for this topic.
    /// </summary>
    public bool IsIdempotent { get; set; } = false;

    /// <summary>
    /// TTL in seconds for idempotency claim records in Redis.
    /// Default 604800 seconds (7 days) provides sufficient deduplication window for most workloads.
    /// </summary>
    public int IdempotencyRetentionSeconds { get; set; } = 604800;

    /// <summary>
    /// Enables encrypted SHA-256 digest verification for this topic.
    /// </summary>
    public bool EnableAuthentication { get; set; } = false;

    /// <summary>
    /// Gets or sets the strategy to use for processing large message payloads.
    /// Defaults to <see langword="null"/>, so no special strategy is applied unless explicitly configured.
    /// </summary>
    public LargePayloadStrategy? LargePayloadStrategy { get; set; }

    /// <summary>
    /// The maximum size of each segment in bytes when using <see cref="LargePayloadStrategy.Segmenting"/>.
    /// Defaults to 512,000 bytes (500 KB).
    /// </summary>
    public int MaxSegmentSizeBytes { get; set; } = 512000;

    /// <summary>
    /// Legacy flag to enable S3 Claim Check offloading. Use <see cref="LargePayloadStrategy"/> instead.
    /// </summary>
    [Obsolete("Use LargePayloadStrategy instead.")]
    public bool? EnableOffloading { get; set; }

    /// <summary>
    /// Legacy flag to enable automatic message segmenting. Use <see cref="LargePayloadStrategy"/> instead.
    /// </summary>
    [Obsolete("Use LargePayloadStrategy instead.")]
    public bool? EnableSegmenting { get; set; }

    /// <summary>
    /// Resolves the active large payload strategy, combining new and legacy properties.
    /// </summary>
    internal LargePayloadStrategy? ResolvedStrategy
    {
        get
        {
            if (LargePayloadStrategy.HasValue)
            {
                return LargePayloadStrategy.Value;
            }
#pragma warning disable CS0618
            if (EnableOffloading == true)
            {
                return CSharpExtensions.Kafka.Abstractions.LargePayloadStrategy.S3Offloading;
            }
            if (EnableSegmenting == true)
            {
                return CSharpExtensions.Kafka.Abstractions.LargePayloadStrategy.Segmenting;
            }
            if (EnableOffloading == false || EnableSegmenting == false)
            {
                return null;
            }
#pragma warning restore CS0618
            return null;
        }
    }

    /// <summary>
    /// Enables Dead Letter Queue routing for failed messages on this topic.
    /// </summary>
    public bool EnableDlq { get; set; } = false;

    /// <summary>
    /// Target DLQ topic name. When empty, defaults to "{TopicName}.dlq".
    /// </summary>
    public string TargetDlqTopic { get; set; } = "";

    /// <summary>
    /// Maximum number of handler execution attempts before routing to DLQ or halting.
    /// Default 5 provides robust retry coverage for transient failures.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;
}
