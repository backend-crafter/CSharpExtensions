namespace CSharpExtensions.Foundation.Helpers.Options;

/// <summary>
/// Configuration for the sharding topology.
/// Bound from the "Sharding" section in appsettings.json.
/// </summary>
public sealed class ShardingOptions
{
    public const string SectionName = "Sharding";

    /// <summary>
    /// Total number of logical shards. Must be a power of two. Default: 128.
    /// </summary>
    public int LogicalShardCount { get; set; } = 128;

    /// <summary>
    /// Hot read model retention in days. Default: 90 for production.
    /// </summary>
    public int HotReadModelRetentionDays { get; set; } = 90;

    /// <summary>
    /// Number of rows to delete per batch during retention cleanup. Default: 5000.
    /// </summary>
    public int CleanupBatchSize { get; set; } = 5000;

    /// <summary>
    /// Delay in milliseconds between cleanup batches to reduce lock contention. Default: 100.
    /// </summary>
    public int CleanupDelayBetweenBatchesMs { get; set; } = 100;
}
