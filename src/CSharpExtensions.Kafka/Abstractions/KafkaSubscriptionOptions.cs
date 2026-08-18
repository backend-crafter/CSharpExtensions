namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Runtime subscription options passed when subscribing to an event topic.
/// </summary>
public sealed class KafkaSubscriptionOptions
{
    /// <summary>
    /// Controls the starting offset for the consumer.
    /// </summary>
    public KafkaReadMode ReadMode { get; set; } = KafkaReadMode.Latest;

    /// <summary>
    /// Overrides the consumer group from topic configuration.
    /// When empty, falls back to <see cref="KafkaTopicConfiguration.GroupId"/>.
    /// </summary>
    public string ConsumerGroup { get; set; } = "";

    /// <summary>
    /// Starting offset timestamp (ISO 8601) or numeric offset for historical replay.
    /// Only used when <see cref="ReadMode"/> is <see cref="KafkaReadMode.HistoricalReplay"/>.
    /// </summary>
    public string StartOffsetTime { get; set; } = "";
}
