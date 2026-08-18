namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Configuration settings for the automated maintenance background service.
/// Handles cleanup of stale assemblies, completed jobs, and permanently failed outbox records.
/// Disabled by default; enable via KafkaBuilder.UseMaintenance or JSON configuration.
/// </summary>
public sealed class KafkaMaintenanceSettings
{

    /// <summary>
    /// Authoritative distributed lock backend used by every maintenance instance.
    /// </summary>
    public KafkaMaintenanceLockProvider LockProvider { get; set; } = KafkaMaintenanceLockProvider.SqlServer;

    /// <summary>
    /// Connection-string name used for SQL Server distributed locking and durable maintenance state.
    /// </summary>
    public string LockConnectionStringName { get; set; } = "DefaultConnection";

    /// <summary>
    /// Interval in minutes between maintenance cycles.
    /// </summary>
    public int IntervalMinutes { get; set; } = 60;

    /// <summary>
    /// Time in seconds after which incomplete message assemblies are considered stale and cleaned up.
    /// </summary>
    public int StaleAssemblyThresholdSeconds { get; set; } = 3600;

    /// <summary>
    /// Number of days to retain completed staged resolve jobs before deletion.
    /// </summary>
    public int CompletedJobRetentionDays { get; set; } = 7;

    /// <summary>
    /// Number of days to retain permanently failed outbox records before deletion.
    /// </summary>
    public int PermanentlyFailedOutboxRetentionDays { get; set; } = 30;

    /// <summary>
    /// Enables index maintenance (weekly defragmentation of database tables).
    /// Disabled by default.
    /// </summary>
    public bool EnableIndexMaintenance { get; set; } = false;

    /// <summary>
    /// Minimum number of hours between shared index-maintenance executions.
    /// </summary>
    public int IndexMaintenanceIntervalHours { get; set; } = 168;

    /// <summary>
    /// SQL command timeout for each index-maintenance operation.
    /// </summary>
    public int IndexCommandTimeoutSeconds { get; set; } = 300;

    /// <summary>
    /// Controls whether real Kafka topic configuration keys are enumerated in the maintenance OpenAPI document.
    /// Disabled by default because OpenAPI endpoints can be exposed independently of controller authorization.
    /// </summary>
    public bool ExposeTopicConfigurationKeysInOpenApi { get; set; }
}

/// <summary>
/// Distributed lock backends supported by the maintenance worker.
/// </summary>
public enum KafkaMaintenanceLockProvider
{
    SqlServer = 0,
    Redis = 1
}
