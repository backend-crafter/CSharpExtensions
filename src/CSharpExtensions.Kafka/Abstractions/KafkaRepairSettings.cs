namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Configuration settings for the database-staged topic repair pipeline.
/// </summary>
public sealed class KafkaRepairSettings
{
    /// <summary>
    /// Gets or sets the name of the connection string key in the ConnectionStrings configuration section.
    /// </summary>
    public string ConnectionStringName { get; set; } = "";

    /// <summary>
    /// Gets or sets the database schema where recovery staging tables are located or will be created.
    /// Defaults to "dbo".
    /// </summary>
    public string TableSchema { get; set; } = "dbo";

    /// <summary>
    /// Gets or sets the batch size for exporting messages from the Kafka topic into the staging database table.
    /// Defaults to 1000.
    /// </summary>
    public int ExportBatchSize { get; set; } = 1000;

    /// <summary>
    /// Gets or sets the batch size for upcasting messages in the staging database table.
    /// Defaults to 1000.
    /// </summary>
    public int UpcastBatchSize { get; set; } = 1000;

    /// <summary>
    /// Gets or sets the batch size for repopulating/republishing messages from the staging table to the target Kafka topic.
    /// Defaults to 500.
    /// </summary>
    public int RepopulateBatchSize { get; set; } = 500;

    /// <summary>
    /// Gets or sets the bounded wait time for the SQL Server session-owned recovery lock.
    /// Defaults to 30 seconds.
    /// </summary>
    public int DistributedLockTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Gets or sets a value indicating whether the staging DDL should be automatically provisioned on startup/execution.
    /// Defaults to true.
    /// </summary>
    public bool AutoProvisionDdl { get; set; } = true;
}
