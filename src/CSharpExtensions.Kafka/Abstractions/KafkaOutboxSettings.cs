namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Configuration settings for the transactional outbox processor background service.
/// Disabled by default; enable via KafkaBuilder.UseOutbox or JSON configuration.
/// </summary>
public sealed class KafkaOutboxSettings
{
    /// <summary>
    /// Enables the transactional outbox processor background service.
    /// When disabled, the KafkaOutboxProcessor will not poll the database.
    /// </summary>
    public bool IsEnabled { get; set; } = false;

    /// <summary>
    /// The name of the connection string key in ConnectionStrings configuration section.
    /// The outbox table (dbo.kafka_outbox) will be auto-provisioned and polled from this database.
    /// </summary>
    public string ConnectionStringName { get; set; } = "DefaultConnection";

    /// <summary>
    /// The database schema where the outbox table is located or will be created.
    /// Defaults to "dbo".
    /// </summary>
    public string TableSchema { get; set; } = "dbo";

    /// <summary>
    /// Number of outbox records to claim and process in a single batch.
    /// Higher values improve throughput for high-volume producers.
    /// </summary>
    public int BatchSize { get; set; } = 200;

    /// <summary>
    /// Maximum publish attempts stored on each newly enqueued outbox record.
    /// </summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>
    /// Delay in milliseconds between polling cycles when no records were processed.
    /// Lower values reduce publishing latency at the cost of database load.
    /// </summary>
    public int PollingIntervalMs { get; set; } = 500;

    /// <summary>
    /// Delay in milliseconds to wait after an unhandled error before retrying the poll loop.
    /// </summary>
    public int ErrorDelayMs { get; set; } = 3000;

    /// <summary>
    /// Duration in seconds for which a claimed outbox row belongs to one processor instance.
    /// Expired claims can be reclaimed after a process crash.
    /// </summary>
    public int ProcessingLeaseSeconds { get; set; } = 300;

    /// <summary>
    /// Base delay in seconds before a failed outbox record becomes claimable again.
    /// </summary>
    public int RetryBaseDelaySeconds { get; set; } = 5;

    /// <summary>
    /// Maximum delay in seconds for exponential outbox retry scheduling.
    /// </summary>
    public int MaxRetryDelaySeconds { get; set; } = 300;
}
