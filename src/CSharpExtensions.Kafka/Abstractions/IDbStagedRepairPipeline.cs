using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Abstractions;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Provides a crash-resilient data recovery and migration pipeline backed by SQL Server.
/// Allows exporting, processing/repairing, upcasting, and repopulating Kafka topics.
/// </summary>
public interface IDbStagedRepairPipeline
{
    /// <summary>
    /// Phase 1: Exports messages from a Kafka topic to the recovery staging table in SQL Server.
    /// Automatically resumes from the maximum offset already present in the database.
    /// </summary>
    /// <param name="topicName">The Kafka topic to read from.</param>
    /// <param name="connectionString">The connection string for SQL Server.</param>
    /// <param name="progressReporter">An optional callback invoked with (processedCount, totalCount) to report progress.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Result wrapping the number of newly exported records.</returns>
    Task<Result<long>> ExportToStagingAsync(
        string topicName,
        string connectionString,
        Action<long, long>? progressReporter = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Phase 2: Repairs staged payloads in transactional batches using custom translation mappers.
    /// </summary>
    /// <param name="connectionString">The connection string for SQL Server.</param>
    /// <param name="repairRules">A delegate defining custom string payload modifications.</param>
    /// <param name="batchSize">The number of rows to process per transactional batch.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A functional Result indicating execution outcome.</returns>
    Task<Result> ProcessStagedRepairsAsync(
        string connectionString,
        Func<string, Result<string>> repairRules,
        int batchSize = 1000,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Phase 3: Automatically upgrades (upcasts) staged payloads to the target message type's latest version
    /// using the registered <c>MessageUpcastRegistry</c>.
    /// </summary>
    /// <typeparam name="TTargetMessage">The target message type to upcast to.</typeparam>
    /// <param name="connectionString">The connection string for SQL Server.</param>
    /// <param name="batchSize">The number of rows to process per batch.</param>
    /// <param name="progressReporter">An optional callback invoked with (processedCount, totalCount) to report progress.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A functional Result indicating execution outcome.</returns>
    Task<Result> UpcastStagedPayloadsAsync<TTargetMessage>(
        string connectionString,
        int batchSize = 1000,
        Action<long, long>? progressReporter = null,
        CancellationToken cancellationToken = default)
        where TTargetMessage : class;

    /// <summary>
    /// Repopulates a Kafka topic from corrected payloads in the staging table.
    /// Progress is committed only upon confirmed broker delivery reports, preventing duplicates.
    /// </summary>
    /// <param name="topicName">The target Kafka topic to republish to.</param>
    /// <param name="connectionString">The connection string for SQL Server.</param>
    /// <param name="batchSize">The number of records to republish per batch.</param>
    /// <param name="progressReporter">An optional callback invoked with (processedCount, totalCount) to report progress.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A functional Result indicating execution outcome.</returns>
    Task<Result> RepopulateTopicFromStagingAsync(
        string topicName,
        string connectionString,
        int batchSize = 500,
        Action<long, long>? progressReporter = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Truncates the staging table associated with the specified topic name.
    /// </summary>
    /// <param name="topicName">The Kafka topic schema to resolve.</param>
    /// <param name="connectionString">The connection string for SQL Server.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A functional Result indicating execution outcome.</returns>
    Task<Result> TruncateStagingTableAsync(
        string topicName,
        string connectionString,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Copies all staged records from one topic's staging table to another's staging table.
    /// </summary>
    /// <param name="sourceTopicName">Source Kafka topic name.</param>
    /// <param name="targetTopicName">Target Kafka topic name.</param>
    /// <param name="connectionString">The connection string for SQL Server.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A functional Result indicating execution outcome.</returns>
    Task<Result> CopyStagedRecordsAsync(
        string sourceTopicName,
        string targetTopicName,
        string connectionString,
        CancellationToken cancellationToken = default);
}
