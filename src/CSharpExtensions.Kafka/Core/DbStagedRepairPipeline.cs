using CSharpExtensions.Foundation.Helpers.Constants;
using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core.Ddl;
using CSharpExtensions.Kafka.Evolution;
using Confluent.Kafka;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Default implementation of the <see cref="IDbStagedRepairPipeline"/> using Dapper and Confluent.Kafka.
/// </summary>
public sealed class DbStagedRepairPipeline : IDbStagedRepairPipeline
{
    private readonly KafkaOptions _options;
    private readonly MessageUpcastRegistry _upcastRegistry;
    private readonly KafkaProducerManager _producerManager;
    private readonly IEnumerable<KafkaRepairConfiguration> _repairConfigurations;
    private readonly ILogger<DbStagedRepairPipeline> _logger;

    public DbStagedRepairPipeline(
        IOptions<KafkaOptions> options,
        MessageUpcastRegistry upcastRegistry,
        KafkaProducerManager producerManager,
        IEnumerable<KafkaRepairConfiguration> repairConfigurations,
        ILogger<DbStagedRepairPipeline> logger)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _upcastRegistry = upcastRegistry ?? throw new ArgumentNullException(nameof(upcastRegistry));
        _producerManager = producerManager ?? throw new ArgumentNullException(nameof(producerManager));
        _repairConfigurations = repairConfigurations ?? throw new ArgumentNullException(nameof(repairConfigurations));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    private string GetTableSchema(string topicName)
    {
        var topicConfigEntry = _options.Topics.FirstOrDefault(t => t.Value.TopicName == topicName);
        if (topicConfigEntry.Key != null)
        {
            var config = _repairConfigurations.FirstOrDefault(rc => rc.TopicConfigurationKey == topicConfigEntry.Key);
            if (config != null)
            {
                return config.Settings.TableSchema;
            }
        }
        return "dbo";
    }

    private string GetTableSchema<TTargetMessage>()
    {
        var config = _repairConfigurations.FirstOrDefault(rc => rc.TargetMessageType == typeof(TTargetMessage));
        if (config != null)
        {
            return config.Settings.TableSchema;
        }
        return "dbo";
    }

    private bool GetAutoProvisionDdl(string topicName)
    {
        var topicConfigEntry = _options.Topics.FirstOrDefault(t => t.Value.TopicName == topicName);
        if (topicConfigEntry.Key != null)
        {
            var config = _repairConfigurations.FirstOrDefault(rc => rc.TopicConfigurationKey == topicConfigEntry.Key);
            if (config != null)
            {
                return config.Settings.AutoProvisionDdl;
            }
        }
        return true;
    }

    /// <inheritdoc />
    public async Task<Result<long>> ExportToStagingAsync(
        string topicName,
        string connectionString,
        Action<long, long>? progressReporter = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName)) throw new ArgumentException("Topic name cannot be empty.", nameof(topicName));
        if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            var topicConfig = _options.Topics.Values.FirstOrDefault(t => t.TopicName == topicName)
                              ?? new KafkaTopicConfiguration { TopicName = topicName };

            var clusterAlias = string.IsNullOrWhiteSpace(topicConfig.Cluster) ? _options.DefaultClusterAlias : topicConfig.Cluster;
            var bootstrapServers = _options.Clusters.TryGetValue(clusterAlias, out var cluster) ? cluster.BootstrapServers : _options.Servers;

            if (string.IsNullOrWhiteSpace(bootstrapServers))
            {
                return Result.Failure<long>("No bootstrap servers resolved for topic.");
            }

            var rawSchema = GetTableSchema(topicName);
            var schema = SqlIdentifierValidator.ValidateIdentifier(rawSchema, nameof(rawSchema));
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Auto-provision table if allowed
            if (GetAutoProvisionDdl(topicName))
            {
                var createTableSql = KafkaRepairStagingDdl.CreateTable(schema);
                await connection.ExecuteAsync(createTableSql);
            }

            // Fetch max offset per partition
            var existingOffsets = (await connection.QueryAsync<(int Partition, long Offset)>(
                $"SELECT partition_id, MAX(message_offset) FROM [{schema}].kafka_repair_staging GROUP BY partition_id"))
                .ToDictionary(x => x.Partition, x => x.Offset);

            // Resolve partitions on broker
            using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);

            if (topicMetadata == null)
            {
                return Result.Failure<long>("The Kafka repair source topic was not found on the broker.");
            }

            var partitions = topicMetadata.Partitions.Select(p => p.PartitionId).ToList();

            var partitionOffsets = partitions.Select(pId =>
            {
                var offset = existingOffsets.TryGetValue(pId, out var maxOffset)
                    ? new Offset(maxOffset + 1)
                    : Offset.Beginning;
                return new TopicPartitionOffset(topicName, pId, offset);
            }).ToList();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                AllowAutoCreateTopics = false,
                GroupId = $"kafka-repair-temp-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            consumer.Assign(partitionOffsets);

            long totalPending = 0;
            foreach (var partition in partitions)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var watermark = consumer.QueryWatermarkOffsets(new TopicPartition(topicName, new Partition(partition)), TimeSpan.FromSeconds(2));
                    var startOffset = existingOffsets.TryGetValue(partition, out var maxOffset)
                        ? Math.Max(maxOffset + 1, watermark.Low.Value)
                        : watermark.Low.Value;
                    var pending = Math.Max(0L, watermark.High.Value - startOffset);
                    totalPending += pending;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    _logger.LogWarning(
                        "Failed to query a watermark offset. ErrorType: {ErrorType}.",
                        exception.GetType().Name);
                }
            }

            progressReporter?.Invoke(0, totalPending);

            long exportedCount = 0;
            var emptyPolls = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (consumeResult == null || consumeResult.IsPartitionEOF)
                {
                    emptyPolls++;
                    if (emptyPolls >= 3)
                    {
                        // Stop if no new messages received after 3 attempts
                        break;
                    }
                    continue;
                }

                emptyPolls = 0;

                var headerSchemaVersion = consumeResult.Message.Headers
                    .FirstOrDefault(h => string.Equals(h.Key, CustomRequestHeaders.EventSchemaVersion, StringComparison.OrdinalIgnoreCase))
                    ?.GetValueBytes();
                var schemaVersion = headerSchemaVersion != null ? Encoding.UTF8.GetString(headerSchemaVersion) : null;

                await connection.ExecuteAsync($@"
                    INSERT INTO [{schema}].kafka_repair_staging 
                        (partition_id, message_offset, message_key, raw_payload, event_schema_version, processing_status)
                    VALUES 
                        (@Partition, @Offset, @Key, @Payload, @SchemaVersion, 'Pending')",
                    new
                    {
                        Partition = consumeResult.Partition.Value,
                        Offset = consumeResult.Offset.Value,
                        Key = consumeResult.Message.Key,
                        Payload = consumeResult.Message.Value,
                        SchemaVersion = schemaVersion
                    });

                exportedCount++;
                progressReporter?.Invoke(exportedCount, totalPending);
            }

            return Result.Success(exportedCount);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError("Error during Phase 1 Export. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure<long>("Kafka recovery export failed.");
        }
    }

    /// <inheritdoc />
    public async Task<Result> ProcessStagedRepairsAsync(
        string connectionString,
        Func<string, Result<string>> repairRules,
        int batchSize = 1000,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));
        if (repairRules is null) throw new ArgumentNullException(nameof(repairRules));

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            const string rawSchema = "dbo";
            var schema = SqlIdentifierValidator.ValidateIdentifier(rawSchema, nameof(rawSchema));
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Rollback orphaned processing tasks
            await connection.ExecuteAsync($@"
                UPDATE [{schema}].kafka_repair_staging
                SET processing_status = 'Pending', updated_at = GETUTCDATE()
                WHERE processing_status = 'Processing'");

            var sqlSelect = $@"
                SELECT TOP (@BatchSize) staging_id, raw_payload 
                FROM [{schema}].kafka_repair_staging WITH (UPDLOCK, ROWLOCK, READPAST)
                WHERE processing_status = 'Pending'";

            while (!cancellationToken.IsCancellationRequested)
            {
                var batch = (await connection.QueryAsync<(long StagingId, string RawPayload)>(
                    sqlSelect, new { BatchSize = batchSize })).ToList();

                if (batch.Count == 0) break;

                foreach (var row in batch)
                {
                    // Lock row
                    await connection.ExecuteAsync($@"
                        UPDATE [{schema}].kafka_repair_staging 
                        SET processing_status = 'Processing', updated_at = GETUTCDATE()
                        WHERE staging_id = @StagingId", new { row.StagingId });

                    var result = repairRules(row.RawPayload);

                    if (result.IsSuccess)
                    {
                        await connection.ExecuteAsync($@"
                            UPDATE [{schema}].kafka_repair_staging 
                            SET corrected_payload = @Corrected, processing_status = 'Completed', updated_at = GETUTCDATE()
                            WHERE staging_id = @StagingId", new { Corrected = result.Value, row.StagingId });
                    }
                    else
                    {
                        await connection.ExecuteAsync($@"
                            UPDATE [{schema}].kafka_repair_staging 
                            SET validation_error = @Error, processing_status = 'Failed', updated_at = GETUTCDATE()
                            WHERE staging_id = @StagingId", new { Error = "Repair rule rejected payload.", row.StagingId });
                    }
                }
            }

            return Result.Success();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError("Error during Phase 2 Process. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure("Kafka recovery processing failed.");
        }
    }

    /// <inheritdoc />
    public async Task<Result> UpcastStagedPayloadsAsync<TTargetMessage>(
        string connectionString,
        int batchSize = 1000,
        Action<long, long>? progressReporter = null,
        CancellationToken cancellationToken = default)
        where TTargetMessage : class
    {
        if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            var rawSchema = GetTableSchema<TTargetMessage>();
            var schema = SqlIdentifierValidator.ValidateIdentifier(rawSchema, nameof(rawSchema));
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Rollback orphaned processing tasks
            await connection.ExecuteAsync($@"
                UPDATE [{schema}].kafka_repair_staging
                SET processing_status = 'Pending', updated_at = GETUTCDATE()
                WHERE processing_status = 'Processing'");

            // Query total count of pending rows
            var totalPending = await connection.ExecuteScalarAsync<long>($@"
                SELECT COUNT(*) FROM [{schema}].kafka_repair_staging WHERE processing_status = 'Pending'");

            long processedCount = 0;
            progressReporter?.Invoke(processedCount, totalPending);

            var targetSchemaKey = typeof(TTargetMessage).Name;
            var sqlSelect = $@"
                SELECT TOP (@BatchSize) staging_id, raw_payload, event_schema_version 
                FROM [{schema}].kafka_repair_staging WITH (UPDLOCK, ROWLOCK, READPAST)
                WHERE processing_status = 'Pending'";

            while (!cancellationToken.IsCancellationRequested)
            {
                var batch = (await connection.QueryAsync<(long StagingId, string RawPayload, string EventSchemaVersion)>(
                    sqlSelect, new { BatchSize = batchSize })).ToList();

                if (batch.Count == 0) break;

                foreach (var row in batch)
                {
                    // Lock row
                    await connection.ExecuteAsync($@"
                        UPDATE [{schema}].kafka_repair_staging 
                        SET processing_status = 'Processing', updated_at = GETUTCDATE()
                        WHERE staging_id = @StagingId", new { row.StagingId });

                    var sourceSchemaKey = string.IsNullOrWhiteSpace(row.EventSchemaVersion) ? targetSchemaKey : row.EventSchemaVersion;
                    var result = _upcastRegistry.UpcastMessage(row.RawPayload, sourceSchemaKey, targetSchemaKey);

                    if (result.IsSuccess)
                    {
                        await connection.ExecuteAsync($@"
                            UPDATE [{schema}].kafka_repair_staging 
                            SET corrected_payload = @Corrected, processing_status = 'Completed', updated_at = GETUTCDATE()
                            WHERE staging_id = @StagingId", new { Corrected = result.Value, row.StagingId });
                    }
                    else
                    {
                        await connection.ExecuteAsync($@"
                            UPDATE [{schema}].kafka_repair_staging 
                            SET validation_error = @Error, processing_status = 'Failed', updated_at = GETUTCDATE()
                            WHERE staging_id = @StagingId", new { Error = "Upcast rejected payload.", row.StagingId });
                    }
                }

                processedCount += batch.Count;
                progressReporter?.Invoke(processedCount, totalPending);
            }

            return Result.Success();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                "Error during Phase 3 Upcast to target message type {TargetType}. ErrorType: {ErrorType}.",
                typeof(TTargetMessage).FullName,
                exception.GetType().Name);
            return Result.Failure("Kafka recovery upcast failed.");
        }
    }

    /// <inheritdoc />
    public async Task<Result> RepopulateTopicFromStagingAsync(
        string topicName,
        string connectionString,
        int batchSize = 500,
        Action<long, long>? progressReporter = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName)) throw new ArgumentException("Topic name cannot be empty.", nameof(topicName));
        if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            var topicConfig = _options.Topics.Values.FirstOrDefault(t => t.TopicName == topicName)
                              ?? new KafkaTopicConfiguration { TopicName = topicName };

            var clusterAlias = string.IsNullOrWhiteSpace(topicConfig.Cluster) ? _options.DefaultClusterAlias : topicConfig.Cluster;
            var targetSchemaKey = topicConfig.TopicName.Split('.').LastOrDefault() ?? "Unknown";

            var rawSchema = GetTableSchema(topicName);
            var schema = SqlIdentifierValidator.ValidateIdentifier(rawSchema, nameof(rawSchema));
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            var totalPending = await connection.ExecuteScalarAsync<long>($@"
                SELECT COUNT(*) FROM [{schema}].kafka_repair_staging 
                WHERE processing_status = 'Completed' AND is_republished = 0");

            long processedCount = 0;
            progressReporter?.Invoke(processedCount, totalPending);

            var sqlSelect = $@"
                SELECT TOP (@BatchSize) staging_id, message_key, corrected_payload 
                FROM [{schema}].kafka_repair_staging 
                WHERE processing_status = 'Completed' AND is_republished = 0";

            while (!cancellationToken.IsCancellationRequested)
            {
                var batch = (await connection.QueryAsync<(long StagingId, string MessageKey, string CorrectedPayload)>(
                    sqlSelect, new { BatchSize = batchSize })).ToList();

                if (batch.Count == 0) break;

                foreach (var row in batch)
                {
                    var headers = new Dictionary<string, string>
                    {
                        [CustomRequestHeaders.MessageId] = Guid.NewGuid().ToString(),
                        [CustomRequestHeaders.CorrelationId] = Guid.NewGuid().ToString(),
                        [CustomRequestHeaders.EventSchemaVersion] = targetSchemaKey
                    };

                    var publishResult = await _producerManager.PublishDirectAsync(
                        topicName,
                        clusterAlias,
                        row.MessageKey,
                        row.CorrectedPayload,
                        headers,
                        topicConfig.Username,
                        topicConfig.Password,
                        cancellationToken);

                    if (!publishResult.IsSuccess)
                    {
                        return Result.Failure("Kafka repair repopulation stopped because a staged record could not be published.");
                    }

                    // Update republish state
                    await connection.ExecuteAsync($@"
                        UPDATE [{schema}].kafka_repair_staging
                        SET is_republished = 1, updated_at = GETUTCDATE()
                        WHERE staging_id = @StagingId", new { row.StagingId });

                    processedCount++;
                    progressReporter?.Invoke(processedCount, totalPending);
                }
            }

            return Result.Success();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError("Error during Phase 4 Repopulate. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure("Kafka recovery repopulation failed.");
        }
    }

    /// <inheritdoc />
    public async Task<Result> TruncateStagingTableAsync(
        string topicName,
        string connectionString,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName)) throw new ArgumentException("Topic name cannot be empty.", nameof(topicName));
        if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            var rawSchema = GetTableSchema(topicName);
            var schema = SqlIdentifierValidator.ValidateIdentifier(rawSchema, nameof(rawSchema));
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            await connection.ExecuteAsync(new CommandDefinition(
                $"TRUNCATE TABLE [{schema}].kafka_repair_staging;",
                cancellationToken: cancellationToken));

            return Result.Success();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError("Failed to truncate Kafka staging table. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure("Kafka staging-table truncation failed.");
        }
    }

    /// <inheritdoc />
    public async Task<Result> CopyStagedRecordsAsync(
        string sourceTopicName,
        string targetTopicName,
        string connectionString,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sourceTopicName)) throw new ArgumentException("Source topic name cannot be empty.", nameof(sourceTopicName));
        if (string.IsNullOrWhiteSpace(targetTopicName)) throw new ArgumentException("Target topic name cannot be empty.", nameof(targetTopicName));
        if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));

        cancellationToken.ThrowIfCancellationRequested();

        var sourceSchema = SqlIdentifierValidator.ValidateIdentifier(GetTableSchema(sourceTopicName), nameof(sourceTopicName));
        var targetSchema = SqlIdentifierValidator.ValidateIdentifier(GetTableSchema(targetTopicName), nameof(targetTopicName));

        if (string.Equals(sourceSchema, targetSchema, StringComparison.OrdinalIgnoreCase))
        {
            return Result.Success(); // Same table, no copy needed
        }

        try
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            await connection.ExecuteAsync(new CommandDefinition($@"
                INSERT INTO [{targetSchema}].kafka_repair_staging 
                (partition_id, message_offset, message_key, raw_payload, event_schema_version, processing_status, created_at, updated_at)
                SELECT partition_id, message_offset, message_key, raw_payload, event_schema_version, processing_status, created_at, updated_at
                FROM [{sourceSchema}].kafka_repair_staging;", cancellationToken: cancellationToken));

            return Result.Success();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError("Failed to copy Kafka staged records. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure("Kafka staged-record copy failed.");
        }
    }
}
