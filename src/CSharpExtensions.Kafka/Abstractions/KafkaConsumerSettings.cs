namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Configuration settings for the Kafka consumer, with defaults optimized for high-throughput scenarios.
/// </summary>
public sealed class KafkaConsumerSettings
{
    /// <summary>
    /// Maximum time to wait for the first successful partition assignment during application startup.
    /// </summary>
    public int StartupTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Maximum number of automatic restart attempts for a crashed consumer loop.
    /// A value of 0 means unlimited restarts (recommended for production).
    /// </summary>
    public int MaxRestartAttempts { get; set; } = 0;

    /// <summary>
    /// Base delay in milliseconds before restarting a crashed consumer.
    /// Each subsequent restart uses exponential backoff from this base value.
    /// </summary>
    public int RestartBaseDelayMs { get; set; } = 1000;

    /// <summary>
    /// Maximum delay in milliseconds for exponential backoff between consumer restarts.
    /// </summary>
    public int MaxRestartDelayMs { get; set; } = 30000;

    /// <summary>
    /// Base delay in milliseconds between message handler retry attempts.
    /// </summary>
    public int HandlerRetryBaseDelayMs { get; set; } = 1000;

    /// <summary>
    /// Maximum delay in milliseconds between message handler retry attempts.
    /// </summary>
    public int HandlerRetryMaxDelayMs { get; set; } = 30000;

    /// <summary>
    /// Session timeout in milliseconds. The consumer is considered dead if no heartbeat is received within this period.
    /// Higher values reduce false-positive rebalances under heavy load.
    /// </summary>
    public int SessionTimeoutMs { get; set; } = 45000;

    /// <summary>
    /// Maximum time in milliseconds between poll invocations before the consumer is removed from the group.
    /// Must be greater than the maximum expected handler processing time.
    /// </summary>
    public int MaxPollIntervalMs { get; set; } = 300000;

    /// <summary>
    /// Delay in milliseconds to wait after a consume error before retrying the poll loop.
    /// </summary>
    public int ConsumeErrorDelayMs { get; set; } = 1000;

    /// <summary>
    /// Minimum number of bytes the broker should accumulate before returning a fetch response.
    /// Higher values improve throughput by reducing network round trips.
    /// </summary>
    public int FetchMinBytes { get; set; } = 1024;

    /// <summary>
    /// Maximum time in milliseconds the broker waits for FetchMinBytes to be available before responding.
    /// </summary>
    public int FetchMaxWaitMs { get; set; } = 500;

    /// <summary>
    /// Maximum number of immediate retries (without delay) before escalating to DLQ.
    /// Use 1 for high-throughput topics, 2-3 for critical low-volume topics.
    /// Unlike exponential backoff, immediate retries do not block the consumer loop.
    /// </summary>
    public int MaxImmediateRetries { get; set; } = 1;

    /// <summary>
    /// Number of handler failures within the sliding window that will trip the circuit breaker.
    /// When tripped, the consumer pauses for <see cref="CircuitBreakerCooldownMs"/> milliseconds.
    /// </summary>
    public int CircuitBreakerFailureThreshold { get; set; } = 5;

    /// <summary>
    /// Duration of the circuit breaker sliding window in seconds.
    /// Failures outside this window are not counted toward the threshold.
    /// </summary>
    public int CircuitBreakerWindowSeconds { get; set; } = 60;

    /// <summary>
    /// Cooldown period in milliseconds when the circuit breaker is open.
    /// After this period, the consumer enters HalfOpen state and processes one probe message.
    /// </summary>
    public int CircuitBreakerCooldownMs { get; set; } = 30000;
}

