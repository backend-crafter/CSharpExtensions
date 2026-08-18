using CSharpExtensions.Foundation.Helpers.Constants;
using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Confluent.Kafka;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using RailwayError = Error;
using TopicMetadata = CSharpExtensions.Kafka.Abstractions.TopicMetadata;

/// <summary>
/// Internal implementation of <see cref="IKafkaMaintenanceService"/>.
/// Orchestrates maintenance operations using existing Kafka infrastructure services.
/// </summary>
internal sealed class KafkaMaintenanceService : IKafkaMaintenanceService
{
    private readonly KafkaOptions _options;
    private readonly IKafkaAdministrationService _administrationService;
    private readonly IKafkaTopicValidator _topicValidator;
    private readonly KafkaProducerManager _producerManager;
    private readonly SignatureService _signatureService;
    private readonly IConfiguration _configuration;
    private readonly ILogger<KafkaMaintenanceService> _logger;

    public KafkaMaintenanceService(
        IOptions<KafkaOptions> options,
        IKafkaAdministrationService administrationService,
        IKafkaTopicValidator topicValidator,
        KafkaProducerManager producerManager,
        SignatureService signatureService,
        IConfiguration configuration,
        ILogger<KafkaMaintenanceService> logger)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _administrationService = administrationService ?? throw new ArgumentNullException(nameof(administrationService));
        _topicValidator = topicValidator ?? throw new ArgumentNullException(nameof(topicValidator));
        _producerManager = producerManager ?? throw new ArgumentNullException(nameof(producerManager));
        _signatureService = signatureService ?? throw new ArgumentNullException(nameof(signatureService));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<Result<int>> ReplayDlqAsync(
        string topicConfigurationKey,
        int maxMessages = 1000,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicConfigurationKey))
        {
            return Result.Failure<int>(
                new RailwayError("Topic configuration key must not be empty.")
                    .AsBadRequest("Validation", "Missing topic configuration key."));
        }

        if (maxMessages is < 1 or > 10000)
        {
            return Result.Failure<int>(
                new RailwayError("Replay batch size must be between 1 and 10000.")
                    .AsBadRequest("Validation", "Replay batch size is outside the allowed range."));
        }

        if (!_options.Topics.TryGetValue(topicConfigurationKey, out var topicConfig))
        {
            return Result.Failure<int>(
                new RailwayError($"Topic configuration '{topicConfigurationKey}' is not defined.")
                    .AsNotFound());
        }

        if (!topicConfig.EnableDlq)
        {
            return Result.Failure<int>(
                new RailwayError($"DLQ is not enabled for topic '{topicConfigurationKey}'.")
                    .AsBadRequest("Configuration", "DLQ is disabled."));
        }

        var dlqTopicName = string.IsNullOrWhiteSpace(topicConfig.TargetDlqTopic)
            ? $"{topicConfig.TopicName}.dlq"
            : topicConfig.TargetDlqTopic;

        var clusterAlias = string.IsNullOrWhiteSpace(topicConfig.Cluster)
            ? _options.DefaultClusterAlias
            : topicConfig.Cluster;

        var checkpointId = Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes($"{clusterAlias}:{topicConfigurationKey}")))[..16].ToLowerInvariant();
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = ResolveBootstrapServers(clusterAlias),
            AllowAutoCreateTopics = false,
            GroupId = $"kafka-dlq-replay-{checkpointId}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            EnablePartitionEof = true,
            IsolationLevel = IsolationLevel.ReadCommitted
        };
        ApplyClusterSecurity(consumerConfig, clusterAlias, topicConfig);

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(dlqTopicName);

        var replayedCount = 0;
        var eofPartitions = new System.Collections.Generic.HashSet<int>();

        _logger.LogInformation(
            "Starting DLQ replay for topic '{DlqTopic}' -> '{SourceTopic}'. Max messages: {MaxMessages}.",
            dlqTopicName, topicConfig.TopicName, maxMessages);

        try
        {
            while (!cancellationToken.IsCancellationRequested && replayedCount < maxMessages)
            {
                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));

                if (consumeResult is null)
                {
                    break;
                }

                if (consumeResult.IsPartitionEOF)
                {
                    eofPartitions.Add(consumeResult.Partition.Value);
                    // Check if all assigned partitions reached EOF
                    if (eofPartitions.Count >= consumer.Assignment.Count)
                    {
                        break;
                    }
                    continue;
                }

                ConsumedMessageHeaders extractedHeaders;
                try
                {
                    extractedHeaders = ConsumedMessageHeaders.Extract<object>(
                        consumeResult,
                        collectRawHeaders: false,
                        allowGeneratedMessageIdFallback: false);
                }
                catch (Exception exception) when (exception is InvalidDataException or OverflowException)
                {
                    return Result.Failure<int>(
                        new RailwayError("DLQ replay stopped because the record contains invalid transport headers.")
                            .CausedBy(exception));
                }

                if (!extractedHeaders.HasValidMessageIdHeader
                    || !TryGetSingleUtf8Header(consumeResult.Message.Headers, CustomRequestHeaders.OriginalTopic, out var originalTopic)
                    || !string.Equals(originalTopic, topicConfig.TopicName, StringComparison.Ordinal)
                    || !TryGetSingleUtf8Header(consumeResult.Message.Headers, CustomRequestHeaders.OriginalPartition, out var originalPartition)
                    || !int.TryParse(originalPartition, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var partitionValue)
                    || partitionValue < 0
                    || !TryGetSingleUtf8Header(consumeResult.Message.Headers, CustomRequestHeaders.OriginalOffset, out var originalOffset)
                    || !long.TryParse(originalOffset, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var offsetValue)
                    || offsetValue < 0)
                {
                    return Result.Failure<int>(
                        new RailwayError("DLQ replay stopped because canonical source provenance is missing or mismatched."));
                }

                if (topicConfig.EnableAuthentication)
                {
                    if (string.IsNullOrWhiteSpace(extractedHeaders.Signature)
                        || !extractedHeaders.Signature.StartsWith("v2.", StringComparison.Ordinal)
                        || !_signatureService.VerifySignature(
                            consumeResult.Message.Value,
                            extractedHeaders.MessageId,
                            extractedHeaders.CorrelationId,
                            dlqTopicName,
                            consumeResult.Message.Key,
                            extractedHeaders.SchemaVersionKey,
                            KafkaEnvelopeKinds.DeadLetter,
                            extractedHeaders.Signature))
                    {
                        return Result.Failure<int>(
                            new RailwayError("DLQ replay is disabled for records without a valid canonical DLQ signature."));
                    }
                }

                var headers = new System.Collections.Generic.Dictionary<string, string>
                {
                    [CustomRequestHeaders.MessageId] = extractedHeaders.MessageId,
                    [CustomRequestHeaders.CorrelationId] = extractedHeaders.CorrelationId,
                    [CustomRequestHeaders.EventSchemaVersion] = extractedHeaders.SchemaVersionKey
                };

                if (topicConfig.EnableAuthentication)
                {
                    headers[CustomRequestHeaders.MessageSignature] = _signatureService.SignMessage(
                        consumeResult.Message.Value,
                        extractedHeaders.MessageId,
                        extractedHeaders.CorrelationId,
                        topicConfig.TopicName,
                        consumeResult.Message.Key,
                        extractedHeaders.SchemaVersionKey,
                        KafkaMessageBus.ResolveEnvelopeKind(consumeResult.Message.Value, topicConfig));
                }

                var publishResult = await _producerManager.PublishDirectAsync(
                    topicConfig.TopicName,
                    clusterAlias,
                    consumeResult.Message.Key,
                    consumeResult.Message.Value,
                    headers,
                    topicConfig.Username,
                    topicConfig.Password,
                    cancellationToken);

                if (publishResult.IsSuccess)
                {
                    consumer.Commit(consumeResult);
                    replayedCount++;
                }
                else
                {
                    _logger.LogError(
                        "Failed to republish DLQ message to '{SourceTopic}': {ErrorMessage}.",
                        topicConfig.TopicName, publishResult.Error.Message);
                    return Result.Failure<int>(
                        new RailwayError("DLQ replay stopped because the source-topic publish was rejected."));
                }
            }
        }
        finally
        {
            try { consumer.Close(); } catch { /* Suppress close exceptions */ }
        }

        _logger.LogInformation(
            "DLQ replay completed for '{DlqTopic}'. Replayed {Count} messages.",
            dlqTopicName, replayedCount);

        return Result.Success(replayedCount);
    }

    private static bool TryGetSingleUtf8Header(Headers? headers, string headerName, out string value)
    {
        value = string.Empty;
        if (headers is null)
        {
            return false;
        }

        byte[]? encoded = null;
        foreach (var header in headers)
        {
            if (!string.Equals(header.Key, headerName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (encoded is not null)
            {
                return false;
            }

            encoded = header.GetValueBytes();
        }

        if (encoded is null || encoded.Length is 0 or > 512)
        {
            return false;
        }

        try
        {
            value = new UTF8Encoding(false, true).GetString(encoded);
            return !string.IsNullOrWhiteSpace(value);
        }
        catch (DecoderFallbackException)
        {
            return false;
        }
    }

    /// <inheritdoc />
    public async Task<Result<int>> PurgeStaleAssembliesAsync(CancellationToken cancellationToken = default)
    {
        if (!_options.Assembly.IsEnabled)
        {
            return Result.Failure<int>(
                new RailwayError("Message assembly is not enabled.")
                    .AsBadRequest("Configuration", "Assembly feature is disabled."));
        }

        if (_options.Assembly.Provider == AssemblyProvider.SqlServer)
        {
            var connectionString = _configuration.GetConnectionString(_options.Assembly.ConnectionStringName) ?? _configuration[_options.Assembly.ConnectionStringName];
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                return Result.Failure<int>(
                    new RailwayError("Kafka assembly connection string is not configured.")
                        .AsInternalServer("Configuration", "Missing connection string."));
            }

            try
            {
                await using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken);

                var schema = SqlIdentifierValidator.ValidateIdentifier(_options.Assembly.TableSchema, nameof(_options.Assembly.TableSchema));
                var retentionThreshold = DateTime.UtcNow.AddSeconds(-_options.Maintenance.StaleAssemblyThresholdSeconds);
                var purgedCount = await connection.ExecuteAsync(
                    $"DELETE FROM [{schema}].[pending_message_assemblies] WHERE created_at < @Threshold",
                    new { Threshold = retentionThreshold });

                _logger.LogInformation("Purged {Count} stale assembly segments from SQL Server.", purgedCount);
                return Result.Success(purgedCount);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (SqlException exception)
            {
                _logger.LogError("Failed to purge stale Kafka assemblies. ErrorType: {ErrorType}.", exception.GetType().Name);
                return Result.Failure<int>(
                    new RailwayError("Failed to purge stale Kafka assemblies.").CausedBy(exception));
            }
        }

        // Redis provider: stale segments auto-expire via TTL
        _logger.LogInformation("Redis assembly segments auto-expire via TTL. No manual purge needed.");
        return Result.Success(0);
    }

    /// <inheritdoc />
    public async Task<Result<int>> RetryDeadLetteredJobsAsync(
        string jobType,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(jobType))
        {
            return Result.Failure<int>(
                new RailwayError("Job type must not be empty.")
                    .AsBadRequest("Validation", "Missing job type."));
        }

        if (!_options.StagedJobs.IsEnabled)
        {
            return Result.Failure<int>(
                new RailwayError("Staged jobs engine is not enabled.")
                    .AsBadRequest("Configuration", "Staged jobs feature is disabled."));
        }

        var connectionString = _configuration.GetConnectionString(_options.StagedJobs.ConnectionStringName) ?? _configuration[_options.StagedJobs.ConnectionStringName];
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return Result.Failure<int>(
                new RailwayError("Kafka staged-jobs connection string is not configured.")
                    .AsInternalServer("Configuration", "Missing connection string."));
        }

        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            var schema = SqlIdentifierValidator.ValidateIdentifier(_options.StagedJobs.TableSchema, nameof(_options.StagedJobs.TableSchema));
            var resetCount = await connection.ExecuteAsync(
                $@"UPDATE [{schema}].[staged_resolve_jobs]
                   SET status = 'Pending', attempt_count = 0, error_message = NULL, updated_at = GETUTCDATE()
                   WHERE job_type = @JobType AND status = 'DeadLetter'",
                new { JobType = jobType });

            _logger.LogInformation("Reset {Count} dead-lettered staged jobs of type '{JobType}' for retry.", resetCount, jobType);
            return Result.Success(resetCount);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (SqlException exception)
        {
            _logger.LogError("Failed to retry dead-lettered Kafka jobs. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure<int>(
                new RailwayError("Failed to retry dead-lettered Kafka jobs.").CausedBy(exception));
        }
    }

    /// <inheritdoc />
    public async Task<Result<TopicValidationReport>> ValidateTopicAsync(
        string topicConfigurationKey,
        int sampleSize = 100,
        CancellationToken cancellationToken = default)
    {
        return await _topicValidator.ValidateAsync(topicConfigurationKey, sampleSize, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<Result<TopicMetadata>> GetTopicMetadataAsync(
        string topicName,
        CancellationToken cancellationToken = default)
    {
        var result = await _administrationService.GetTopicMetadataAsync(topicName, clusterAlias: null, cancellationToken);
        return result;
    }

    /// <inheritdoc />
    public async Task<Result<int>> GetPendingOutboxCountAsync(CancellationToken cancellationToken = default)
    {
        if (!_options.Outbox.IsEnabled)
        {
            return Result.Failure<int>(
                new RailwayError("Outbox is not enabled.")
                    .AsBadRequest("Configuration", "Outbox feature is disabled."));
        }

        var connectionStringName = _options.Outbox.ConnectionStringName;
        if (string.IsNullOrWhiteSpace(connectionStringName))
        {
            return Result.Failure<int>(
                new RailwayError("Outbox connection string is not configured.")
                    .AsInternalServer("Configuration", "Missing connection string."));
        }

        var names = connectionStringName.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var totalCount = 0;

        foreach (var name in names)
        {
            var connectionString = _configuration.GetConnectionString(name) ?? _configuration[name];
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                return Result.Failure<int>(
                    new RailwayError("Kafka outbox connection string is not configured.")
                        .AsInternalServer("Configuration", "Missing connection string."));
            }

            try
            {
                await using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken);

                var schema = SqlIdentifierValidator.ValidateIdentifier(_options.Outbox.TableSchema, nameof(_options.Outbox.TableSchema));
                var count = await connection.ExecuteScalarAsync<int>(
                    $"SELECT COUNT(*) FROM [{schema}].[kafka_outbox] WHERE processing_status = 'Pending'");

                totalCount += count;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (SqlException exception)
            {
                _logger.LogError("Failed to query pending Kafka outbox count. ErrorType: {ErrorType}.", exception.GetType().Name);
                return Result.Failure<int>(
                    new RailwayError("Failed to query pending Kafka outbox count.").CausedBy(exception));
            }
        }

        return Result.Success(totalCount);
    }

    /// <inheritdoc />
    public async Task<Result> RebuildIndexesAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Maintenance: Rebuilding database indexes for all Kafka infrastructure tables.");

        try
        {
            if (_options.Assembly.IsEnabled && _options.Assembly.Provider == AssemblyProvider.SqlServer)
            {
                var assemblyResult = await RebuildTableIndexesInternalAsync(
                    _options.Assembly.ConnectionStringName, 
                    _options.Assembly.TableSchema, 
                    "pending_message_assemblies", 
                    cancellationToken);
                if (!assemblyResult.IsSuccess) return assemblyResult;
            }

            if (_options.StagedJobs.IsEnabled)
            {
                var stagedJobsResult = await RebuildTableIndexesInternalAsync(
                    _options.StagedJobs.ConnectionStringName, 
                    _options.StagedJobs.TableSchema, 
                    "staged_resolve_jobs", 
                    cancellationToken);
                if (!stagedJobsResult.IsSuccess) return stagedJobsResult;
            }

            if (_options.Outbox.IsEnabled)
            {
                var outboxResult = await RebuildTableIndexesInternalAsync(
                    _options.Outbox.ConnectionStringName, 
                    _options.Outbox.TableSchema, 
                    "kafka_outbox", 
                    cancellationToken);
                if (!outboxResult.IsSuccess) return outboxResult;
            }

            return Result.Success();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError("Maintenance index operation failed. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure(new RailwayError("Kafka index maintenance failed.").CausedBy(exception));
        }
    }

    private async Task<Result> RebuildTableIndexesInternalAsync(
        string connectionStringName,
        string tableSchema,
        string tableName,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(connectionStringName))
        {
            return Result.Failure(
                new RailwayError("Kafka index maintenance database configuration is unavailable.")
                    .AsInternalServer("Configuration", "Missing maintenance database configuration."));
        }

        var names = connectionStringName.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (names.Length == 0)
        {
            return Result.Failure(
                new RailwayError("Kafka index maintenance database configuration is unavailable.")
                    .AsInternalServer("Configuration", "Missing maintenance database configuration."));
        }

        var resolvedConnections = new List<string>(names.Length);
        foreach (var name in names)
        {
            var connectionString = _configuration.GetConnectionString(name) ?? _configuration[name];
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                return Result.Failure(
                    new RailwayError("Kafka index maintenance database configuration is unavailable.")
                        .AsInternalServer("Configuration", "Missing maintenance database configuration."));
            }

            resolvedConnections.Add(connectionString);
        }

        foreach (var connectionString in resolvedConnections)
        {
            try
            {
                var schema = SqlIdentifierValidator.ValidateIdentifier(tableSchema, nameof(tableSchema));
                var validatedTableName = SqlIdentifierValidator.ValidateIdentifier(tableName, nameof(tableName));

                await using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken);

                var tableExistsSql = $"SELECT OBJECT_ID('[{schema}].[{validatedTableName}]', 'U');";
                var objectId = await connection.ExecuteScalarAsync<int?>(
                    new CommandDefinition(tableExistsSql, cancellationToken: cancellationToken));

                if (!objectId.HasValue)
                {
                    return Result.Failure(
                        new RailwayError("Kafka index maintenance table is unavailable.")
                            .AsInternalServer("DatabaseSchema", "Required Kafka maintenance table is missing."));
                }

                const string fragmentedIndexesSql = """
                    SELECT i.name
                    FROM sys.dm_db_index_physical_stats(DB_ID(), @ObjectId, NULL, NULL, 'LIMITED') AS stats
                    INNER JOIN sys.indexes AS i
                        ON i.object_id = stats.object_id AND i.index_id = stats.index_id
                    WHERE stats.page_count >= 1000
                      AND stats.avg_fragmentation_in_percent >= 30.0
                      AND i.index_id > 0
                      AND i.is_disabled = 0;
                    """;
                var indexNames = await connection.QueryAsync<string>(new CommandDefinition(
                    fragmentedIndexesSql,
                    new { ObjectId = objectId.Value },
                    commandTimeout: _options.Maintenance.IndexCommandTimeoutSeconds,
                    cancellationToken: cancellationToken));

                foreach (var indexName in indexNames)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var validatedIndexName = SqlIdentifierValidator.ValidateIdentifier(indexName, nameof(indexName));
                    var rebuildSql = $"ALTER INDEX [{validatedIndexName}] ON [{schema}].[{validatedTableName}] REBUILD;";
                    await connection.ExecuteAsync(new CommandDefinition(
                        rebuildSql,
                        commandTimeout: _options.Maintenance.IndexCommandTimeoutSeconds,
                        cancellationToken: cancellationToken));
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (SqlException exception)
            {
                _logger.LogError(
                    "Maintenance SQL index operation failed. ErrorType: {ErrorType}.",
                    exception.GetType().Name);
                return Result.Failure(
                    new RailwayError("Kafka SQL index maintenance failed.")
                        .AsInternalServer("Database", "Kafka index maintenance failed."));
            }
        }

        return Result.Success();
    }

    private string ResolveBootstrapServers(string clusterAlias)
    {
        if (_options.Clusters.TryGetValue(clusterAlias, out var clusterConfig)
            && !string.IsNullOrWhiteSpace(clusterConfig.BootstrapServers))
        {
            return clusterConfig.BootstrapServers;
        }

        return _options.Servers;
    }

    private void ApplyClusterSecurity(
        ConsumerConfig consumerConfig,
        string clusterAlias,
        KafkaTopicConfiguration topicConfig)
    {
        if (!_options.Clusters.TryGetValue(clusterAlias, out var clusterConfig))
        {
            return;
        }

        if (!string.IsNullOrWhiteSpace(clusterConfig.SecurityProtocol) &&
            Enum.TryParse<SecurityProtocol>(clusterConfig.SecurityProtocol, true, out var securityProtocol))
        {
            consumerConfig.SecurityProtocol = securityProtocol;
        }

        if (!string.IsNullOrWhiteSpace(clusterConfig.SaslMechanism) &&
            Enum.TryParse<SaslMechanism>(clusterConfig.SaslMechanism, true, out var saslMechanism))
        {
            consumerConfig.SaslMechanism = saslMechanism;
        }

        var username = string.IsNullOrWhiteSpace(topicConfig.Username)
            ? clusterConfig.SaslUsername
            : topicConfig.Username;
        var password = string.IsNullOrWhiteSpace(topicConfig.Password)
            ? clusterConfig.SaslPassword
            : topicConfig.Password;

        if (!string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(password))
        {
            consumerConfig.SaslUsername = username;
            consumerConfig.SaslPassword = password;
        }
    }
}
