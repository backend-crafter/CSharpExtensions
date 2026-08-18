namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Defines the consumer read modes for subscribing to Kafka topics.
/// </summary>
public enum KafkaReadMode
{
    Latest,
    HistoricalReplay
}
