using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Abstractions;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Provides programmatic access to Kafka infrastructure maintenance operations.
/// Enables replay of dead-letter queues, cleanup of stale resources,
/// retry of failed staged jobs, and topic integrity validation.
/// </summary>
public interface IKafkaMaintenanceService
{
    /// <summary>
    /// Replays all messages from a Dead Letter Queue topic back to the original source topic.
    /// Messages are re-published with their original headers and metadata intact.
    /// </summary>
    /// <param name="topicConfigurationKey">
    /// The configuration key of the topic whose DLQ should be replayed
    /// (e.g., <c>"SmsCampaignDispatched"</c>). The DLQ topic name is resolved
    /// from <c>TargetDlqTopic</c> or defaults to <c>{TopicName}.dlq</c>.
    /// </param>
    /// <param name="maxMessages">
    /// The maximum number of messages to replay in a single operation.
    /// Use this to prevent overwhelming the source topic with a large backlog.
    /// </param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>
    /// A <see cref="Result{T}"/> wrapping the count of successfully replayed messages.
    /// Returns failure if the topic configuration is not found or DLQ is not enabled.
    /// </returns>
    Task<Result<int>> ReplayDlqAsync(
        string topicConfigurationKey,
        int maxMessages = 1000,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Purges stale or incomplete message assembly segments that have exceeded their retention period.
    /// Cleans up both Redis-backed and SQL Server-backed assembly stores.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>
    /// A <see cref="Result{T}"/> wrapping the count of purged assembly groups.
    /// </returns>
    Task<Result<int>> PurgeStaleAssembliesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Retries all dead-lettered staged jobs of a specific type.
    /// Resets job status from <c>DeadLetter</c> back to <c>Pending</c> and zeroes the attempt counter.
    /// </summary>
    /// <param name="jobType">
    /// The job type identifier to retry (e.g., <c>"ResolveWagerFact"</c>).
    /// Must match the <see cref="IStagedJobExecutor.JobType"/> of a registered executor.
    /// </param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>
    /// A <see cref="Result{T}"/> wrapping the count of jobs reset for retry.
    /// </returns>
    Task<Result<int>> RetryDeadLetteredJobsAsync(
        string jobType,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Validates topic data integrity by scanning a sample of recent messages for
    /// structural correctness, required header compliance, and signature verification.
    /// Delegates to <see cref="IKafkaTopicValidator"/> internally.
    /// </summary>
    /// <param name="topicConfigurationKey">
    /// The configuration key of the topic to validate (e.g., <c>"SmsCampaignDispatched"</c>).
    /// </param>
    /// <param name="sampleSize">
    /// The maximum number of messages to scan during validation.
    /// </param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>
    /// A <see cref="Result{T}"/> wrapping the <see cref="TopicValidationReport"/>
    /// containing the validation summary and any detected structural errors.
    /// </returns>
    Task<Result<TopicValidationReport>> ValidateTopicAsync(
        string topicConfigurationKey,
        int sampleSize = 100,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves metadata for a specific Kafka topic including partition count,
    /// replication factor, and ISR (In-Sync Replicas) status.
    /// Delegates to <see cref="IKafkaAdministrationService"/> internally.
    /// </summary>
    /// <param name="topicName">The physical Kafka topic name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>
    /// A <see cref="Result{T}"/> wrapping the <see cref="TopicMetadata"/> record.
    /// </returns>
    Task<Result<TopicMetadata>> GetTopicMetadataAsync(
        string topicName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the count of pending outbox records that have not yet been published to Kafka.
    /// Useful for monitoring outbox health and detecting processing stalls.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>
    /// A <see cref="Result{T}"/> wrapping the count of pending outbox records.
    /// </returns>
    Task<Result<int>> GetPendingOutboxCountAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Rebuilds database indexes for the Outbox, Staged Jobs, and Message Assemblies tables
    /// to prevent fragmentation and optimize query execution plans.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A railway-oriented Result indicating maintenance success.</returns>
    Task<Result> RebuildIndexesAsync(CancellationToken cancellationToken = default);
}
