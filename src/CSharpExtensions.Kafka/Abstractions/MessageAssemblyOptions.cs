namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Configuration options for the message assembly feature.
/// </summary>
public sealed class MessageAssemblyOptions
{
    /// <summary>
    /// Enables the message assembly feature. Disabled by default.
    /// </summary>
    public bool IsEnabled { get; set; } = false;

    /// <summary>
    /// The assembly provider to use.
    /// </summary>
    public AssemblyProvider Provider { get; set; } = AssemblyProvider.Redis;

    /// <summary>
    /// Redis connection alias for the assembly provider.
    /// Must match an alias registered via KafkaBuilder.AddRedisConnection().
    /// </summary>
    public string RedisConnectionAlias { get; set; } = "Default";

    /// <summary>
    /// The name of the connection string for SQL Server fallback provider.
    /// </summary>
    public string ConnectionStringName { get; set; } = "DefaultConnection";

    /// <summary>
    /// The database schema for the pending_message_assemblies table.
    /// </summary>
    public string TableSchema { get; set; } = "dbo";

    /// <summary>
    /// Time in seconds after which incomplete assemblies are considered stale and cleaned up.
    /// </summary>
    public int StaleThresholdSeconds { get; set; } = 3600;

    /// <summary>
    /// Controls whether the SQL table is automatically provisioned on startup.
    /// Set to false in production where DBA manages schema changes.
    /// </summary>
    public bool AutoProvisionDdl { get; set; } = true;
}
