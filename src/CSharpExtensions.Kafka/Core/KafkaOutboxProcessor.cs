using System.Data;
using CSharpExtensions.Core.Helpers.Constants;
using CSharpExtensions.Kafka.Abstractions;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.Kafka.Core;

/// <summary>
/// Background worker that polls the local outbox table and publishes events to Kafka.
/// Self-disables when <see cref="KafkaOutboxSettings.IsEnabled"/> is false.
/// </summary>
public sealed class KafkaOutboxProcessor : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly KafkaProducerManager _producerManager;
    private readonly SignatureService _signatureService;
    private readonly S3ClaimCheckOffloader _offloader;
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaOutboxProcessor> _logger;

    public KafkaOutboxProcessor(
        IConfiguration configuration,
        KafkaProducerManager producerManager,
        SignatureService signatureService,
        S3ClaimCheckOffloader offloader,
        IOptions<KafkaOptions> options,
        ILogger<KafkaOutboxProcessor> logger)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _producerManager = producerManager ?? throw new ArgumentNullException(nameof(producerManager));
        _signatureService = signatureService ?? throw new ArgumentNullException(nameof(signatureService));
        _offloader = offloader ?? throw new ArgumentNullException(nameof(offloader));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Predefined delay tiers for empty outbox batches (gradual step-up).
    /// </summary>
    private static readonly int[] EmptyBatchDelayMultipliers = [1, 2, 5, 10, 30, 60];

    /// <summary>
    /// Maximum delay in milliseconds for error exponential backoff.
    /// </summary>
    private const int MaxErrorDelayMs = 60000;
    internal const string SqlProvisioningLockAcquireCommand = """
        DECLARE @LockResult int;
        EXEC @LockResult = sys.sp_getapplock
            @Resource = @Resource,
            @LockMode = 'Exclusive',
            @LockOwner = 'Session',
            @LockTimeout = 30000;
        SELECT @LockResult;
        """;

    /// <inheritdoc />
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_options.Outbox.IsEnabled)
        {
            ResolveRequiredConnectionStrings();
        }

        return base.StartAsync(cancellationToken);
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Outbox.IsEnabled)
        {
            _logger.LogInformation("Kafka Outbox Processor is disabled via configuration (Outbox.IsEnabled = false).");
            return;
        }

        _logger.LogInformation("Kafka Outbox Processor background worker started.");

        var connections = ResolveRequiredConnectionStrings();
        foreach (var (_, connectionString) in connections)
        {
            await EnsureOutboxTableExistsAsync(connectionString, stoppingToken);
        }

        await Task.WhenAll(connections.Select(connection =>
            RunPollingLoopAsync(connection.Name, connection.ConnectionString, stoppingToken)));

        _logger.LogInformation("Kafka Outbox Processor background worker stopped.");
    }

    private IReadOnlyList<(string Name, string ConnectionString)> ResolveRequiredConnectionStrings()
    {
        var configuredNames = _options.Outbox.ConnectionStringName;
        if (string.IsNullOrWhiteSpace(configuredNames))
        {
            _logger.LogCritical("Transactional Outbox is enabled but its connection string name is empty.");
            throw new InvalidOperationException("Transactional Outbox requires a configured connection string name.");
        }

        var connectionNames = configuredNames.Split(
            ',',
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (connectionNames.Length == 0)
        {
            throw new InvalidOperationException("Transactional Outbox requires a configured connection string name.");
        }

        var connections = new List<(string Name, string ConnectionString)>(connectionNames.Length);
        foreach (var connectionStringName in connectionNames.Distinct(StringComparer.Ordinal))
        {
            var connectionString = _configuration.GetConnectionString(connectionStringName)
                ?? _configuration[connectionStringName];
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                _logger.LogCritical("Transactional Outbox is enabled but a required SQL Server connection string is unavailable.");
                throw new InvalidOperationException(
                    "Transactional Outbox requires every configured SQL Server connection string to be available.");
            }

            connections.Add((connectionStringName, connectionString));
        }

        return connections;
    }

    private async Task RunPollingLoopAsync(string connectionName, string connectionString, CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting Outbox polling loop for connection '{ConnectionName}'.", connectionName);

        var emptyBatchTierIndex = 0;
        var consecutiveErrorCount = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processedAny = await ProcessOutboxBatchAsync(connectionString, stoppingToken);

                // Reset error backoff on any successful cycle (batch or empty)
                consecutiveErrorCount = 0;

                if (processedAny)
                {
                    // Successful batch: reset to base interval for rapid polling
                    emptyBatchTierIndex = 0;
                }
                else
                {
                    // Empty batch: apply gradual backoff (1s -> 2s -> 5s -> 10s -> 30s -> 60s)
                    var delayMs = CalculateEmptyBatchDelayMs(
                        _options.Outbox.PollingIntervalMs,
                        emptyBatchTierIndex);
                    await Task.Delay(delayMs, stoppingToken);

                    if (emptyBatchTierIndex < EmptyBatchDelayMultipliers.Length - 1)
                    {
                        emptyBatchTierIndex++;
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    "Unhandled exception in Kafka Outbox background loop for connection '{ConnectionName}'. ErrorType: {ErrorType}.",
                    connectionName,
                    exception.GetType().Name);

                // Exponential backoff on errors: 1s -> 2s -> 4s -> 8s -> ... -> 60s max
                consecutiveErrorCount++;
                var errorDelayMs = CalculateErrorDelayMs(
                    _options.Outbox.ErrorDelayMs,
                    consecutiveErrorCount);
                try
                {
                    await Task.Delay(errorDelayMs, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        _logger.LogInformation("Stopped Outbox polling loop for connection '{ConnectionName}'.", connectionName);
    }

    internal static int CalculateEmptyBatchDelayMs(int baseDelayMs, int tierIndex)
    {
        var boundedIndex = Math.Clamp(tierIndex, 0, EmptyBatchDelayMultipliers.Length - 1);
        return (int)Math.Min(
            (long)Math.Max(1, baseDelayMs) * EmptyBatchDelayMultipliers[boundedIndex],
            MaxErrorDelayMs);
    }

    internal static int CalculateErrorDelayMs(int baseDelayMs, int consecutiveErrorCount)
    {
        var exponent = Math.Clamp(consecutiveErrorCount - 1, 0, 30);
        var multiplier = 1L << exponent;
        return (int)Math.Min((long)Math.Max(1, baseDelayMs) * multiplier, MaxErrorDelayMs);
    }

    internal static bool IsProvisioningLockAcquired(int result) => result >= 0;

    /// <summary>
    /// Ensures the outbox infrastructure table exists in the target database.
    /// Auto-creates dbo.kafka_outbox and its performance index if missing.
    /// </summary>
    private async Task EnsureOutboxTableExistsAsync(string connectionString, CancellationToken cancellationToken)
    {
        try
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            var schema = SqlIdentifierValidator.ValidateIdentifier(_options.Outbox.TableSchema, nameof(_options.Outbox.TableSchema));
            var provisioningLockResource = $"csharpextensions.kafka.outbox.provision.{schema}";
            var lockResult = await connection.ExecuteScalarAsync<int>(new CommandDefinition(
                SqlProvisioningLockAcquireCommand,
                new { Resource = provisioningLockResource },
                cancellationToken: cancellationToken));
            if (!IsProvisioningLockAcquired(lockResult))
            {
                throw new InvalidOperationException("Kafka Outbox provisioning lock could not be acquired.");
            }

            try
            {
            var sql = $@"
                IF NOT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = 'kafka_outbox'
                )
                BEGIN
                    CREATE TABLE [{schema}].kafka_outbox (
                        outbox_id          BIGINT IDENTITY(1,1) NOT NULL,
                        message_id         NVARCHAR(100)        NOT NULL,
                        correlation_id     NVARCHAR(100)        NOT NULL,
                        configuration_key  NVARCHAR(200)        NOT NULL,
                        message_key        NVARCHAR(500)        NULL,
                        payload_json       NVARCHAR(MAX)        NOT NULL,
                        headers_json       NVARCHAR(MAX)        NULL,
                        processing_status  NVARCHAR(50)         NOT NULL DEFAULT 'Pending',
                        attempt_count      INT                  NOT NULL DEFAULT 0,
                        max_attempts       INT                  NOT NULL DEFAULT 5,
                        error_message      NVARCHAR(MAX)        NULL,
                        processing_owner   NVARCHAR(64)         NULL,
                        lease_expires_at   DATETIME2            NULL,
                        claim_version      BIGINT               NOT NULL DEFAULT 0,
                        next_attempt_at    DATETIME2            NULL,
                        created_at         DATETIME2            NOT NULL DEFAULT GETUTCDATE(),
                        updated_at         DATETIME2            NOT NULL DEFAULT GETUTCDATE(),
                        CONSTRAINT PK_kafka_outbox PRIMARY KEY CLUSTERED (outbox_id ASC)
                    );

                    CREATE NONCLUSTERED INDEX IX_kafka_outbox_status
                        ON [{schema}].kafka_outbox (processing_status, outbox_id)
                        WHERE processing_status IN ('Pending', 'Failed');
                END;

                IF COL_LENGTH('[{schema}].kafka_outbox', 'processing_owner') IS NULL
                    ALTER TABLE [{schema}].kafka_outbox ADD processing_owner NVARCHAR(64) NULL;

                IF COL_LENGTH('[{schema}].kafka_outbox', 'lease_expires_at') IS NULL
                    ALTER TABLE [{schema}].kafka_outbox ADD lease_expires_at DATETIME2 NULL;

                IF COL_LENGTH('[{schema}].kafka_outbox', 'claim_version') IS NULL
                    ALTER TABLE [{schema}].kafka_outbox
                        ADD claim_version BIGINT NOT NULL DEFAULT (0) WITH VALUES;

                IF COL_LENGTH('[{schema}].kafka_outbox', 'next_attempt_at') IS NULL
                    ALTER TABLE [{schema}].kafka_outbox ADD next_attempt_at DATETIME2 NULL;

                IF NOT EXISTS (
                    SELECT 1
                    FROM sys.indexes
                    WHERE object_id = OBJECT_ID('[{schema}].kafka_outbox')
                      AND name = 'IX_kafka_outbox_claimable'
                )
                BEGIN
                    CREATE NONCLUSTERED INDEX IX_kafka_outbox_claimable
                        ON [{schema}].kafka_outbox (processing_status, lease_expires_at, outbox_id)
                        INCLUDE (attempt_count, max_attempts);
                END;

                IF NOT EXISTS (
                    SELECT 1
                    FROM sys.indexes
                    WHERE object_id = OBJECT_ID('[{schema}].kafka_outbox')
                      AND name = 'IX_kafka_outbox_retry_claimable'
                )
                BEGIN
                    CREATE NONCLUSTERED INDEX IX_kafka_outbox_retry_claimable
                        ON [{schema}].kafka_outbox (processing_status, next_attempt_at, lease_expires_at, outbox_id)
                        INCLUDE (attempt_count, max_attempts);
                END;";

            await connection.ExecuteAsync(
                new CommandDefinition(sql, new { Schema = schema }, cancellationToken: cancellationToken));

            var checkSql = $"SELECT OBJECT_ID('[{schema}].kafka_outbox', 'U');";
            var objectId = await connection.ExecuteScalarAsync<int?>(
                new CommandDefinition(checkSql, cancellationToken: cancellationToken));

            if (objectId.HasValue)
            {
                _logger.LogInformation("Kafka Outbox table [{Schema}].kafka_outbox is provisioned and ready.", schema);
            }
            else
            {
                _logger.LogCritical("Failed to provision Kafka Outbox table [{Schema}].kafka_outbox. The processor cannot start.", schema);
                throw new InvalidOperationException("Kafka Outbox table provisioning failed.");
            }
            }
            finally
            {
                try
                {
                    await connection.ExecuteAsync(new CommandDefinition(
                        "EXEC sp_releaseapplock @Resource = @Resource, @LockOwner = 'Session';",
                        new { Resource = provisioningLockResource },
                        cancellationToken: CancellationToken.None));
                }
                catch (Exception exception)
                {
                    _logger.LogWarning(
                        "Failed to release Kafka Outbox provisioning lock. ErrorType: {ErrorType}.",
                        exception.GetType().Name);
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (InvalidOperationException)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogCritical("Failed to auto-provision Kafka Outbox table. ErrorType: {ErrorType}.", exception.GetType().Name);
            throw;
        }
    }

    private async Task<bool> ProcessOutboxBatchAsync(string connectionString, CancellationToken cancellationToken)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        using var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var schema = SqlIdentifierValidator.ValidateIdentifier(_options.Outbox.TableSchema, nameof(_options.Outbox.TableSchema));
            var processingOwner = Guid.NewGuid().ToString("N");
            var selectSql = $@"
                UPDATE [{schema}].kafka_outbox
                SET processing_status = 'Processing',
                    processing_owner = @ProcessingOwner,
                    lease_expires_at = DATEADD(SECOND, @LeaseSeconds, GETUTCDATE()),
                    claim_version = claim_version + 1,
                    updated_at = GETUTCDATE()
                OUTPUT inserted.outbox_id AS OutboxId, 
                       inserted.message_id AS MessageId, 
                       inserted.correlation_id AS CorrelationId, 
                       inserted.configuration_key AS ConfigurationKey, 
                       inserted.message_key AS MessageKey, 
                       inserted.payload_json AS PayloadJson, 
                       inserted.headers_json AS HeadersJson, 
                       inserted.processing_status AS ProcessingStatus, 
                       inserted.attempt_count AS AttemptCount, 
                       inserted.max_attempts AS MaxAttempts,
                       inserted.processing_owner AS ProcessingOwner,
                       inserted.claim_version AS ClaimVersion
                WHERE outbox_id IN (
                    SELECT TOP (@BatchSize) outbox_id 
                    FROM [{schema}].kafka_outbox WITH (UPDLOCK, ROWLOCK, READPAST)
                    WHERE (((processing_status IN ('Pending', 'Failed'))
                            AND (next_attempt_at IS NULL OR next_attempt_at <= GETUTCDATE()))
                           OR (processing_status = 'Processing' AND lease_expires_at < GETUTCDATE()))
                      AND attempt_count < max_attempts
                    ORDER BY outbox_id ASC
                );";

            var claimedRecords = await connection.QueryAsync<OutboxRecord>(
                new CommandDefinition(
                    selectSql,
                    new
                    {
                        BatchSize = _options.Outbox.BatchSize,
                        ProcessingOwner = processingOwner,
                        LeaseSeconds = Math.Clamp(_options.Outbox.ProcessingLeaseSeconds, 30, 3600)
                    },
                    transaction: transaction,
                    cancellationToken: cancellationToken));

            var recordList = claimedRecords as IReadOnlyList<OutboxRecord> ?? claimedRecords.ToList();
            if (recordList.Count == 0)
            {
                transaction.Commit();
                return false;
            }

            transaction.Commit();

            foreach (var record in recordList)
            {
                await ProcessRecordAsync(connectionString, record, cancellationToken);
            }

            return true;
        }
        catch (Exception exception)
        {
            try
            {
                transaction.Rollback();
            }
            catch
            {
                // Suppress rollback errors
            }

            if (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                _logger.LogError(
                    "Failed processing database outbox batch transaction. ErrorType: {ErrorType}.",
                    exception.GetType().Name);
            }

            throw;
        }
    }

    private async Task ProcessRecordAsync(
        string connectionString,
        OutboxRecord record, 
        CancellationToken cancellationToken)
    {
        var schema = SqlIdentifierValidator.ValidateIdentifier(_options.Outbox.TableSchema, nameof(_options.Outbox.TableSchema));
        try
        {
            if (!await RenewOutboxLeaseAsync(connectionString, schema, record, cancellationToken))
            {
                throw new OutboxLeaseLostException();
            }

            using var processingCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var publishTask = PublishRecordAsync(record, processingCancellation.Token);
            var renewalTask = RenewOutboxLeaseUntilCancelledAsync(
                connectionString,
                schema,
                record,
                processingCancellation.Token);
            var completedTask = await Task.WhenAny(publishTask, renewalTask);
            if (completedTask == renewalTask)
            {
                processingCancellation.Cancel();
                try
                {
                    await publishTask;
                }
                catch (OperationCanceledException) when (processingCancellation.IsCancellationRequested)
                {
                }

                await renewalTask;
                throw new OutboxLeaseLostException();
            }

            try
            {
                await publishTask;
            }
            finally
            {
                processingCancellation.Cancel();
                try
                {
                    await renewalTask;
                }
                catch (OperationCanceledException) when (processingCancellation.IsCancellationRequested)
                {
                }
            }

            await DeleteOwnedRecordAsync(connectionString, schema, record, cancellationToken);
        }
        catch (OutboxLeaseLostException)
        {
            _logger.LogCritical(
                "Outbox ownership fence was lost for record {OutboxId}. The stale processor will not mutate the row.",
                record.OutboxId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            await MarkRecordFailureAsync(connectionString, schema, record, exception, cancellationToken);
        }
    }

    private async Task PublishRecordAsync(OutboxRecord record, CancellationToken cancellationToken)
    {
        if (!_options.Topics.TryGetValue(record.ConfigurationKey, out var topicConfig))
        {
            throw new InvalidOperationException($"Kafka configuration key '{record.ConfigurationKey}' is not defined in options.");
        }

        var finalPayload = record.PayloadJson;
        var isOffloadedReference = false;

        if (topicConfig.ResolvedStrategy == LargePayloadStrategy.S3Offloading)
        {
            var byteCount = System.Text.Encoding.UTF8.GetByteCount(record.PayloadJson);
            if (byteCount > _options.Offloading.InlineThresholdBytes)
            {
                var offloadResult = await _offloader.OffloadAsync(
                    record.PayloadJson,
                    record.ConfigurationKey,
                    _options.Offloading,
                    cancellationToken);

                if (!offloadResult.IsSuccess)
                {
                        throw new InvalidOperationException("Claim check offloading failed.");
                }

                finalPayload = offloadResult.Value;
                isOffloadedReference = true;
            }
        }

        var headers = new Dictionary<string, string>
        {
            [CustomRequestHeaders.MessageId] = record.MessageId,
            [CustomRequestHeaders.CorrelationId] = record.CorrelationId,
            [CustomRequestHeaders.EventSchemaVersion] = record.ConfigurationKey
        };

        if (topicConfig.EnableAuthentication)
        {
            headers[CustomRequestHeaders.MessageSignature] = _signatureService.SignMessage(
                finalPayload,
                record.MessageId,
                record.CorrelationId,
                topicConfig.TopicName,
                record.MessageKey,
                record.ConfigurationKey,
                isOffloadedReference ? KafkaEnvelopeKinds.S3Reference : KafkaEnvelopeKinds.Inline);
        }

        var clusterAlias = string.IsNullOrWhiteSpace(topicConfig.Cluster)
            ? _options.DefaultClusterAlias
            : topicConfig.Cluster;

        var publishResult = await _producerManager.PublishDirectAsync(
            topicConfig.TopicName,
            clusterAlias,
            record.MessageKey,
            finalPayload,
            headers,
            topicConfig.Username,
            topicConfig.Password,
            cancellationToken);

        if (!publishResult.IsSuccess)
        {
            throw new InvalidOperationException("Kafka broker publish rejected the outbox record.");
        }
    }

    private async Task<bool> RenewOutboxLeaseAsync(
        string connectionString,
        string schema,
        OutboxRecord record,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        var affected = await connection.ExecuteAsync(new CommandDefinition(
            $@"UPDATE [{schema}].kafka_outbox
               SET lease_expires_at = DATEADD(SECOND, @LeaseSeconds, GETUTCDATE()),
                   updated_at = GETUTCDATE()
               WHERE outbox_id = @OutboxId
                 AND processing_owner = @ProcessingOwner
                 AND claim_version = @ClaimVersion
                 AND processing_status = 'Processing'
                 AND lease_expires_at >= GETUTCDATE();",
            new
            {
                record.OutboxId,
                record.ProcessingOwner,
                record.ClaimVersion,
                LeaseSeconds = _options.Outbox.ProcessingLeaseSeconds
            },
            cancellationToken: cancellationToken));
        return affected == 1;
    }

    private async Task RenewOutboxLeaseUntilCancelledAsync(
        string connectionString,
        string schema,
        OutboxRecord record,
        CancellationToken cancellationToken)
    {
        var intervalSeconds = Math.Max(5, _options.Outbox.ProcessingLeaseSeconds / 3);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(intervalSeconds));
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            if (!await RenewOutboxLeaseAsync(connectionString, schema, record, cancellationToken))
            {
                throw new OutboxLeaseLostException();
            }
        }
    }

    private static async Task DeleteOwnedRecordAsync(
        string connectionString,
        string schema,
        OutboxRecord record,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        var affected = await connection.ExecuteAsync(new CommandDefinition(
            $@"DELETE FROM [{schema}].kafka_outbox
               WHERE outbox_id = @OutboxId
                 AND processing_owner = @ProcessingOwner
                 AND claim_version = @ClaimVersion
                 AND processing_status = 'Processing'
                 AND lease_expires_at >= GETUTCDATE();",
            new { record.OutboxId, record.ProcessingOwner, record.ClaimVersion },
            cancellationToken: cancellationToken));
        if (affected != 1)
        {
            throw new OutboxLeaseLostException();
        }
    }

    private async Task MarkRecordFailureAsync(
        string connectionString,
        string schema,
        OutboxRecord record,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var newAttemptCount = record.AttemptCount + 1;
        var safeError = exception.GetType().Name;
        var permanentlyFailed = newAttemptCount >= record.MaxAttempts;
        var retryDelaySeconds = CalculateRetryDelaySeconds(
            _options.Outbox.RetryBaseDelaySeconds,
            _options.Outbox.MaxRetryDelaySeconds,
            newAttemptCount);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        var affected = await connection.ExecuteAsync(new CommandDefinition(
            $@"UPDATE [{schema}].kafka_outbox
               SET processing_status = @ProcessingStatus,
                   attempt_count = @AttemptCount,
                   error_message = @ErrorMessage,
                   processing_owner = NULL,
                   lease_expires_at = NULL,
                   next_attempt_at = CASE WHEN @PermanentlyFailed = 1
                       THEN NULL ELSE DATEADD(SECOND, @RetryDelaySeconds, GETUTCDATE()) END,
                   updated_at = GETUTCDATE()
               WHERE outbox_id = @OutboxId
                 AND processing_owner = @ProcessingOwner
                 AND claim_version = @ClaimVersion
                 AND processing_status = 'Processing'
                 AND lease_expires_at >= GETUTCDATE();",
            new
            {
                record.OutboxId,
                record.ProcessingOwner,
                record.ClaimVersion,
                AttemptCount = newAttemptCount,
                ErrorMessage = safeError,
                ProcessingStatus = permanentlyFailed ? "PermanentlyFailed" : "Failed",
                PermanentlyFailed = permanentlyFailed,
                RetryDelaySeconds = retryDelaySeconds
            },
            cancellationToken: cancellationToken));
        if (affected != 1)
        {
            _logger.LogCritical(
                "Outbox failure update was fenced out for record {OutboxId}. The stale processor will not retry the mutation.",
                record.OutboxId);
            return;
        }

        if (permanentlyFailed)
        {
            _logger.LogCritical(
                "Kafka Outbox record reached max attempts and has failed permanently. ErrorType: {ErrorType}.",
                safeError);
        }
        else
        {
            _logger.LogWarning(
                "Outbox record scheduled for retry attempt {AttemptCount} after {RetryDelaySeconds} seconds. ErrorType: {ErrorType}.",
                newAttemptCount,
                retryDelaySeconds,
                safeError);
        }
    }

    internal static int CalculateRetryDelaySeconds(int baseDelaySeconds, int maxDelaySeconds, int attemptCount)
    {
        var exponent = Math.Clamp(attemptCount - 1, 0, 30);
        var delay = (long)Math.Max(1, baseDelaySeconds) * (1L << exponent);
        return (int)Math.Min(delay, Math.Max(1, maxDelaySeconds));
    }

    private sealed class OutboxLeaseLostException : Exception
    {
    }
}
