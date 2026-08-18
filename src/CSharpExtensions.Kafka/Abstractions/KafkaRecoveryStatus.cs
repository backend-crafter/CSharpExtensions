namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Represents the progress status of a running background topic recovery/migration process.
/// </summary>
public sealed class KafkaRecoveryStatus
{
    /// <summary>
    /// Gets or sets the name of the topic being recovered.
    /// </summary>
    public string TopicName { get; set; } = "";

    /// <summary>
    /// Gets or sets the current phase.
    /// E.g. "Exporting", "Processing", "Upcasting", "Repopulating", "Completed", "Failed".
    /// </summary>
    public string Phase { get; set; } = "Pending";

    /// <summary>
    /// Gets or sets the number of messages processed in the current phase.
    /// </summary>
    public long ProcessedCount { get; set; }

    /// <summary>
    /// Gets or sets the total estimated messages to process in the current phase.
    /// </summary>
    public long TotalCount { get; set; }

    /// <summary>
    /// Gets or sets the error message if the recovery process failed.
    /// </summary>
    public string? ErrorMessage { get; set; }
}
