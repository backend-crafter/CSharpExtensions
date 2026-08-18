using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Abstractions;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Provides topic provisioning and metadata operations for Kafka clusters.
/// </summary>
public interface IKafkaAdministrationService
{
    /// <summary>
    /// Creates a new Kafka topic with the specified configuration.
    /// </summary>
    /// <param name="topicName">The fully qualified topic name.</param>
    /// <param name="partitionCount">Number of partitions to create.</param>
    /// <param name="replicationFactor">Replication factor for the topic.</param>
    /// <param name="topicConfigs">Optional topic-level configuration overrides (e.g., retention.ms).</param>
    /// <param name="clusterAlias">Optional cluster alias. Uses default if not specified.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Result indicating success or failure with error details.</returns>
    Task<Result> CreateTopicAsync(
        string topicName,
        int partitionCount,
        short replicationFactor,
        Dictionary<string, string>? topicConfigs = null,
        string? clusterAlias = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes an existing Kafka topic.
    /// </summary>
    /// <param name="topicName">The fully qualified topic name to delete.</param>
    /// <param name="clusterAlias">Optional cluster alias. Uses default if not specified.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Result indicating success or failure.</returns>
    Task<Result> DeleteTopicAsync(
        string topicName,
        string? clusterAlias = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves metadata for the specified topic.
    /// </summary>
    /// <param name="topicName">The fully qualified topic name.</param>
    /// <param name="clusterAlias">Optional cluster alias. Uses default if not specified.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Result containing topic metadata or failure details.</returns>
    Task<Result<TopicMetadata>> GetTopicMetadataAsync(
        string topicName,
        string? clusterAlias = null,
        CancellationToken cancellationToken = default);
}
