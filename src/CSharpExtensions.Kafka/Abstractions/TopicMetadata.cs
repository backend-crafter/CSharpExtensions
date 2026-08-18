namespace CSharpExtensions.Kafka.Abstractions;

using System.Collections.Generic;

/// <summary>
/// Metadata describing a Kafka topic's structure.
/// </summary>
public sealed record TopicMetadata(
    string TopicName,
    int PartitionCount,
    int ReplicationFactor,
    IReadOnlyDictionary<string, string> Configuration);
