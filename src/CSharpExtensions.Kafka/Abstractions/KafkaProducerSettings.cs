namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Configuration settings for the Kafka producer, with defaults optimized for high-throughput scenarios.
/// </summary>
public sealed class KafkaProducerSettings
{
    /// <summary>
    /// Maximum UTF-8 payload size accepted by the direct producer API.
    /// </summary>
    public int MaxPayloadBytes { get; set; } = 10 * 1024 * 1024;

    /// <summary>
    /// Maximum UTF-8 message-key size accepted by the direct producer API.
    /// </summary>
    public int MaxMessageKeyBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Maximum number of native Kafka producers retained for distinct cluster and credential combinations.
    /// Existing producers remain cached for the manager lifetime; new combinations fail closed at capacity.
    /// </summary>
    public int MaxCachedProducers { get; set; } = 128;

    /// <summary>
    /// When true, a null message key is sent to Kafka as null. The default preserves the legacy
    /// behavior of converting null keys to an empty string.
    /// </summary>
    public bool PreserveNullMessageKey { get; set; }

    /// <summary>
    /// Maximum number of application-level retries for transient client errors
    /// that are not handled by the underlying librdkafka retry mechanism.
    /// </summary>
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>
    /// Base delay in milliseconds for exponential backoff between application-level retries.
    /// </summary>
    public int RetryBaseDelayMs { get; set; } = 100;

    /// <summary>
    /// Maximum delay in milliseconds for exponential backoff between application-level retries.
    /// </summary>
    public int MaxRetryDelayMs { get; set; } = 5000;

    /// <summary>
    /// Delay in milliseconds to wait for additional messages before sending a batch.
    /// Higher values improve throughput at the cost of latency.
    /// Default 50ms is optimized for high-throughput workloads.
    /// </summary>
    public int LingerMs { get; set; } = 50;

    /// <summary>
    /// Prevents broker-side duplicates caused by producer retries within one producer session.
    /// This does not provide end-to-end exactly-once processing.
    /// </summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>
    /// Compression algorithm for message payloads.
    /// Valid values: None, Gzip, Snappy, Lz4, Zstd.
    /// Snappy offers the best speed-to-compression ratio for high throughput.
    /// </summary>
    public string CompressionType { get; set; } = "Snappy";

    /// <summary>
    /// Maximum time in milliseconds for a message to be delivered before timing out.
    /// Default 30s provides sufficient headroom for high-load spikes.
    /// </summary>
    public int MessageTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Maximum number of messages allowed on the producer internal queue.
    /// Default 1M messages supports burst-heavy workloads.
    /// </summary>
    public int QueueBufferingMaxMessages { get; set; } = 1000000;
}
