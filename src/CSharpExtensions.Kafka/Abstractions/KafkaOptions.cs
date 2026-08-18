namespace CSharpExtensions.Kafka.Abstractions;

using System.Collections.Generic;

/// <summary>
/// Root configuration options for the autonomous Kafka library.
/// Binds to the "Kafka" section in application configuration.
/// </summary>
public sealed class KafkaOptions
{
    /// <summary>
    /// Fallback bootstrap servers used when a topic's cluster alias is not found in <see cref="Clusters"/>.
    /// </summary>
    public string Servers { get; set; } = "";

    /// <summary>
    /// Default cluster alias to use when a topic does not specify a cluster.
    /// </summary>
    public string DefaultClusterAlias { get; set; } = "Default";

    /// <summary>
    /// Named cluster configurations keyed by cluster alias.
    /// </summary>
    public Dictionary<string, KafkaClusterConfiguration> Clusters { get; set; } = new();

    /// <summary>
    /// Named topic configurations keyed by event type name (e.g., class name of the event).
    /// </summary>
    public Dictionary<string, KafkaTopicConfiguration> Topics { get; set; } = new();

    /// <summary>
    /// S3 Claim Check offloading settings for large payloads.
    /// </summary>
    public KafkaOffloadOptions Offloading { get; set; } = new();

    /// <summary>
    /// Producer performance and retry settings.
    /// </summary>
    public KafkaProducerSettings Producer { get; set; } = new();

    /// <summary>
    /// Consumer supervision and performance settings.
    /// </summary>
    public KafkaConsumerSettings Consumer { get; set; } = new();

    /// <summary>
    /// Transactional outbox processor settings.
    /// </summary>
    public KafkaOutboxSettings Outbox { get; set; } = new();

    /// <summary>
    /// Staged resolve job engine settings. Disabled by default.
    /// Enable via <c>KafkaBuilder.UseStagedJobs(connectionStringName)</c> or JSON configuration.
    /// </summary>
    public StagedJobSettings StagedJobs { get; set; } = new();

    /// <summary>
    /// Redis-backed idempotency (deduplication) settings.
    /// </summary>
    public IdempotencyOptions Idempotency { get; set; } = new();

    /// <summary>
    /// Multi-segment message assembly settings. Disabled by default.
    /// Enable via <c>KafkaBuilder.UseMessageAssembly()</c> or JSON configuration.
    /// </summary>
    public MessageAssemblyOptions Assembly { get; set; } = new();

    /// <summary>
    /// Automated maintenance task settings. Disabled by default.
    /// Enable via <c>KafkaBuilder.UseMaintenance()</c> or JSON configuration.
    /// </summary>
    public KafkaMaintenanceSettings Maintenance { get; set; } = new();

    /// <summary>
    /// Message authentication and signature migration settings.
    /// </summary>
    public KafkaSecuritySettings Security { get; set; } = new();

    /// <summary>
    /// Enables strict 5/6 segment naming convention validation for topics and consumer groups.
    /// Defaults to <c>true</c>. Set to <c>false</c> to permit legacy topics.
    /// </summary>
    public bool StrictTopicNaming { get; set; } = true;
}
