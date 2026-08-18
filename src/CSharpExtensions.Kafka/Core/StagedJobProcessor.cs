namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core.Ddl;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background hosted service that polls the staged_resolve_jobs table and dispatches
/// jobs to registered <see cref="IStagedJobExecutor"/> implementations.
/// Self-disables when <see cref="StagedJobSettings.IsEnabled"/> is false.
/// </summary>
internal sealed class StagedJobProcessor : BackgroundService
{
    private readonly string? _connectionString;
    private readonly KafkaOptions _options;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<StagedJobProcessor> _logger;
    private readonly string _instanceId;

    /// <summary>
    /// Initializes a new instance of the <see cref="StagedJobProcessor"/> class.
    /// </summary>
    /// <param name="configuration">The application configuration for resolving connection strings.</param>
    /// <param name="options">The Kafka options containing staged job settings.</param>
    /// <param name="serviceProvider">The root service provider for resolving scoped executors.</param>
    /// <param name="logger">The logger instance.</param>
    public StagedJobProcessor(
        IConfiguration configuration,
        IOptions<KafkaOptions> options,
        IServiceProvider serviceProvider,
        ILogger<StagedJobProcessor> logger)
    {
        if (configuration is null) throw new ArgumentNullException(nameof(configuration));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var connectionStringName = _options.StagedJobs.ConnectionStringName;
        _connectionString = configuration.GetConnectionString(connectionStringName) ?? configuration[connectionStringName];
        _instanceId = $"{Environment.MachineName}:{Process.GetCurrentProcess().Id}";
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.StagedJobs.IsEnabled)
        {
            _logger.LogInformation("Staged Job Processor is disabled via configuration (StagedJobs.IsEnabled = false).");
            return;
        }

        if (string.IsNullOrWhiteSpace(_connectionString))
        {
            _logger.LogWarning(
                "SQL Server connection string '{ConnectionStringName}' is not configured. Staged Job Processor will run in standby/disabled mode.",
                _options.StagedJobs.ConnectionStringName);
            return;
        }

        _logger.LogInformation("Staged Job Processor background worker started. Instance: {InstanceId}.", _instanceId);

        if (_options.StagedJobs.AutoProvisionDdl)
        {
            await EnsureTableExistsAsync(stoppingToken);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processedAny = await ProcessJobBatchAsync(stoppingToken);

                if (!processedAny)
                {
                    await Task.Delay(_options.StagedJobs.PollingIntervalMs, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Service is shutting down
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    "Unhandled exception in Staged Job Processor background loop. ErrorType: {ErrorType}.",
                    exception.GetType().Name);
                await Task.Delay(_options.StagedJobs.ErrorDelayMs, stoppingToken);
            }
        }

        _logger.LogInformation("Staged Job Processor background worker stopped.");
    }

    /// <summary>
    /// Ensures the staged_resolve_jobs table exists in the target database.
    /// </summary>
    private async Task EnsureTableExistsAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            var schema = SqlIdentifierValidator.ValidateIdentifier(_options.StagedJobs.TableSchema, nameof(_options.StagedJobs.TableSchema));
            var ddl = ResolveJobsDdl.CreateTable(schema);

            await connection.ExecuteAsync(
                new CommandDefinition(ddl, cancellationToken: cancellationToken));

            _logger.LogInformation(
                "Staged resolve jobs table [{Schema}].staged_resolve_jobs is provisioned and ready.",
                schema);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Failed to auto-provision staged_resolve_jobs table. Ensure the service account has CREATE TABLE permissions.");
            throw;
        }
    }

    /// <summary>
    /// Claims and processes a batch of pending/retry jobs.
    /// </summary>
    /// <returns>True if any jobs were processed, false otherwise.</returns>
    private async Task<bool> ProcessJobBatchAsync(CancellationToken cancellationToken)
    {
        var schema = SqlIdentifierValidator.ValidateIdentifier(_options.StagedJobs.TableSchema, nameof(_options.StagedJobs.TableSchema));
        var settings = _options.StagedJobs;

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Claim a batch of jobs using optimistic locking hints
        var claimSql = $@"
            UPDATE TOP (@BatchSize) [{schema}].staged_resolve_jobs
            WITH (UPDLOCK, ROWLOCK, READPAST)
            SET status = 'Processing',
                locked_by = @InstanceId,
                updated_at = GETUTCDATE()
            OUTPUT inserted.job_id AS JobId,
                   inserted.job_type AS JobType,
                   inserted.payload_json AS PayloadJson,
                   inserted.attempt_count AS AttemptCount,
                   inserted.max_attempts AS MaxAttempts
            WHERE status IN ('Pending', 'Retry')
              AND next_attempt_at <= GETUTCDATE();";

        var claimedJobs = await connection.QueryAsync<StagedJobRecord>(
            new CommandDefinition(
                claimSql,
                new { BatchSize = settings.BatchSize, InstanceId = _instanceId },
                cancellationToken: cancellationToken));

        var jobList = claimedJobs.ToList();
        if (jobList.Count == 0)
        {
            return false;
        }

        _logger.LogDebug("Claimed {JobCount} staged jobs for processing.", jobList.Count);

        foreach (var job in jobList)
        {
            await ProcessSingleJobAsync(connection, job, settings, schema, cancellationToken);
        }

        return true;
    }

    /// <summary>
    /// Processes a single staged job by resolving the appropriate executor.
    /// </summary>
    private async Task ProcessSingleJobAsync(
        SqlConnection connection,
        StagedJobRecord job,
        StagedJobSettings settings,
        string schema,
        CancellationToken cancellationToken)
    {
        try
        {
            using var scope = _serviceProvider.CreateScope();

            // Resolve all registered executors and find the one matching this job type
            var executors = scope.ServiceProvider.GetServices<IStagedJobExecutor>();
            var executor = executors.FirstOrDefault(
                executorInstance => string.Equals(executorInstance.JobType, job.JobType, StringComparison.OrdinalIgnoreCase));

            if (executor is null)
            {
                _logger.LogError(
                    "No IStagedJobExecutor registered for job type '{JobType}'. Job {JobId} will be dead-lettered.",
                    job.JobType,
                    job.JobId);

                await UpdateJobStatusAsync(
                    connection,
                    schema,
                    job.JobId,
                    "DeadLetter",
                    job.AttemptCount + 1,
                    $"No executor registered for job type '{job.JobType}'.",
                    cancellationToken);
                return;
            }

            var result = await executor.ExecuteAsync(job.PayloadJson, cancellationToken);

            if (result.IsSuccess)
            {
                var completeSql = $@"
                    UPDATE [{schema}].staged_resolve_jobs
                    SET status = 'Completed',
                        attempt_count = @AttemptCount,
                        locked_by = NULL,
                        updated_at = GETUTCDATE()
                    WHERE job_id = @JobId;";

                await connection.ExecuteAsync(
                    new CommandDefinition(
                        completeSql,
                        new { JobId = job.JobId, AttemptCount = job.AttemptCount + 1 },
                        cancellationToken: cancellationToken));

                _logger.LogInformation(
                    "Staged job {JobId} (type: '{JobType}') completed successfully.",
                    job.JobId,
                    job.JobType);
            }
            else
            {
                const string errorMessage = "Staged job handler returned a failure.";
                await HandleJobFailureAsync(
                    connection, schema, job, settings, errorMessage, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                "Exception executing staged Kafka job. ErrorType: {ErrorType}.",
                exception.GetType().Name);

            await HandleJobFailureAsync(
                connection, schema, job, settings, "Staged job handler threw an exception.", cancellationToken);
        }
    }

    /// <summary>
    /// Handles a job failure by incrementing the attempt count and applying exponential backoff
    /// or dead-lettering the job if max attempts are reached.
    /// </summary>
    private async Task HandleJobFailureAsync(
        SqlConnection connection,
        string schema,
        StagedJobRecord job,
        StagedJobSettings settings,
        string errorMessage,
        CancellationToken cancellationToken)
    {
        var newAttemptCount = job.AttemptCount + 1;
        var configuredJobMaxAttempts = job.MaxAttempts > 0 ? job.MaxAttempts : settings.MaxAttempts;
        var effectiveMaxAttempts = Math.Clamp(configuredJobMaxAttempts, 1, settings.MaxAttempts);

        if (newAttemptCount >= effectiveMaxAttempts)
        {
            _logger.LogError(
                "Staged job {JobId} (type: '{JobType}') reached max attempts ({MaxAttempts}) and has been dead-lettered. Last error: {ErrorMessage}.",
                job.JobId,
                job.JobType,
                effectiveMaxAttempts,
                errorMessage);

            await UpdateJobStatusAsync(
                connection, schema, job.JobId, "DeadLetter", newAttemptCount, errorMessage, cancellationToken);
        }
        else
        {
            // Exponential backoff: 2^attempt * base delay (ErrorDelayMs)
            var backoffDelayMs = CalculateBoundedRetryDelayMs(
                settings.ErrorDelayMs,
                settings.MaxRetryDelayMs,
                newAttemptCount);
            var nextAttemptAt = DateTime.UtcNow.AddMilliseconds(backoffDelayMs);

            _logger.LogWarning(
                "Staged job {JobId} (type: '{JobType}') failed on attempt {AttemptCount}/{MaxAttempts}. Next retry at {NextAttemptAt:O}. Error: {ErrorMessage}.",
                job.JobId,
                job.JobType,
                newAttemptCount,
                effectiveMaxAttempts,
                nextAttemptAt,
                errorMessage);

            var retrySql = $@"
                UPDATE [{schema}].staged_resolve_jobs
                SET status = 'Retry',
                    attempt_count = @AttemptCount,
                    next_attempt_at = @NextAttemptAt,
                    error_message = @ErrorMessage,
                    locked_by = NULL,
                    updated_at = GETUTCDATE()
                WHERE job_id = @JobId;";

            await connection.ExecuteAsync(
                new CommandDefinition(
                    retrySql,
                    new
                    {
                        JobId = job.JobId,
                        AttemptCount = newAttemptCount,
                        NextAttemptAt = nextAttemptAt,
                        ErrorMessage = errorMessage
                    },
                    cancellationToken: cancellationToken));
        }
    }

    internal static int CalculateBoundedRetryDelayMs(int baseDelayMs, int maxDelayMs, int exponent)
    {
        var delay = baseDelayMs;
        for (var index = 0; index < exponent && delay < maxDelayMs; index++)
        {
            delay = (int)Math.Min((long)maxDelayMs, (long)delay * 2);
        }

        return delay;
    }

    /// <summary>
    /// Updates the job status to a terminal state (DeadLetter or Completed).
    /// </summary>
    private static async Task UpdateJobStatusAsync(
        SqlConnection connection,
        string schema,
        long jobId,
        string status,
        int attemptCount,
        string errorMessage,
        CancellationToken cancellationToken)
    {
        var updateSql = $@"
            UPDATE [{schema}].staged_resolve_jobs
            SET status = @Status,
                attempt_count = @AttemptCount,
                error_message = @ErrorMessage,
                locked_by = NULL,
                updated_at = GETUTCDATE()
            WHERE job_id = @JobId;";

        await connection.ExecuteAsync(
            new CommandDefinition(
                updateSql,
                new
                {
                    JobId = jobId,
                    Status = status,
                    AttemptCount = attemptCount,
                    ErrorMessage = errorMessage
                },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Internal record for mapping claimed job rows from the database.
    /// </summary>
    private sealed class StagedJobRecord
    {
        /// <summary>
        /// The unique job identifier.
        /// </summary>
        public long JobId { get; set; }

        /// <summary>
        /// The job type identifier for executor resolution.
        /// </summary>
        public string JobType { get; set; } = "";

        /// <summary>
        /// The job payload as JSON.
        /// </summary>
        public string PayloadJson { get; set; } = "";

        /// <summary>
        /// The current attempt count.
        /// </summary>
        public int AttemptCount { get; set; }

        /// <summary>
        /// The maximum number of attempts allowed.
        /// </summary>
        public int MaxAttempts { get; set; }
    }
}
