namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Configuration options for the staged resolve job engine.
/// </summary>
public sealed class StagedJobSettings
{
    /// <summary>
    /// Enables the staged job processing engine. Disabled by default.
    /// </summary>
    public bool IsEnabled { get; set; } = false;

    /// <summary>
    /// The name of the connection string for the jobs database.
    /// </summary>
    public string ConnectionStringName { get; set; } = "DefaultConnection";

    /// <summary>
    /// The database schema for the staged_resolve_jobs table.
    /// </summary>
    public string TableSchema { get; set; } = "dbo";

    /// <summary>
    /// Number of jobs to claim and process in a single batch.
    /// </summary>
    public int BatchSize { get; set; } = 50;

    /// <summary>
    /// Polling interval in milliseconds when no jobs are found.
    /// </summary>
    public int PollingIntervalMs { get; set; } = 1000;

    /// <summary>
    /// Delay in milliseconds after an error before retrying.
    /// </summary>
    public int ErrorDelayMs { get; set; } = 5000;

    /// <summary>
    /// Maximum delay in milliseconds for staged-job exponential retry scheduling.
    /// </summary>
    public int MaxRetryDelayMs { get; set; } = 300000;

    /// <summary>
    /// Maximum number of execution attempts before moving to DeadLetter status.
    /// </summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>
    /// Controls whether the SQL table is automatically provisioned on startup.
    /// </summary>
    public bool AutoProvisionDdl { get; set; } = true;
}
