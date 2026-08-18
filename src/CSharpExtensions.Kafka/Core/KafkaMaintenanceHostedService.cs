namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

/// <summary>
/// Background maintenance service that periodically cleans up stale data across
/// message assemblies, staged jobs, and the transactional outbox.
/// Acquires a distributed lock through one backend selected at startup
/// to ensure only one instance runs maintenance in a multi-node deployment.
/// </summary>
internal sealed class KafkaMaintenanceHostedService : BackgroundService
{
    private readonly IRedisConnectionResolver _redisConnectionResolver;
    private readonly KafkaOptions _options;
    private readonly IConfiguration _configuration;
    private readonly List<(string Name, string ConnStr)> _assemblyConnections = new();
    private readonly List<(string Name, string ConnStr)> _stagedJobConnections = new();
    private readonly List<(string Name, string ConnStr)> _outboxConnections = new();
    private readonly (string Name, string ConnStr)? _sqlLockDatabase;
    private readonly ILogger<KafkaMaintenanceHostedService> _logger;
    private readonly string _instanceId;
    private readonly bool _useRedisLock;
    private SqlConnection? _sqlLockConnection;
    private const string LockKeyPrefix = "kafka:maintenance:lock";
    internal const string SqlLockAcquireCommand = """
        DECLARE @LockResult int;
        EXEC @LockResult = sys.sp_getapplock
            @Resource = @Resource,
            @LockMode = 'Exclusive',
            @LockOwner = 'Session',
            @LockTimeout = 0;
        SELECT @LockResult;
        """;
    private static readonly TimeSpan LockTtl = TimeSpan.FromMinutes(5);

    private const string LastIndexMaintenanceKey = "kafka:maintenance:last_index_rebuild";

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaMaintenanceHostedService"/> class.
    /// </summary>
    /// <param name="redisConnectionResolver">The Redis connection resolver for distributed locking.</param>
    /// <param name="options">The Kafka options containing maintenance settings.</param>
    /// <param name="configuration">The application configuration for resolving connection strings.</param>
    /// <param name="logger">The logger instance.</param>
    public KafkaMaintenanceHostedService(
        IRedisConnectionResolver redisConnectionResolver,
        IOptions<KafkaOptions> options,
        IConfiguration configuration,
        ILogger<KafkaMaintenanceHostedService> logger)
    {
        _redisConnectionResolver = redisConnectionResolver ?? throw new ArgumentNullException(nameof(redisConnectionResolver));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        if (_options.Assembly.IsEnabled && _options.Assembly.Provider == AssemblyProvider.SqlServer)
        {
            ResolveConnectionStrings(_options.Assembly.ConnectionStringName, _assemblyConnections);
        }
        if (_options.StagedJobs.IsEnabled)
        {
            ResolveConnectionStrings(_options.StagedJobs.ConnectionStringName, _stagedJobConnections);
        }
        if (_options.Outbox.IsEnabled)
        {
            ResolveConnectionStrings(_options.Outbox.ConnectionStringName, _outboxConnections);
        }
        _instanceId = $"{Environment.MachineName}:{Process.GetCurrentProcess().Id}";
        _useRedisLock = _options.Maintenance.LockProvider == KafkaMaintenanceLockProvider.Redis;
        if (_useRedisLock && !_redisConnectionResolver.IsRegistered())
        {
            throw new InvalidOperationException("Redis is the configured Kafka maintenance lock provider, but no default Redis connection is registered.");
        }

        if (!_useRedisLock)
        {
            var lockName = _options.Maintenance.LockConnectionStringName;
            var lockConnectionString = configuration.GetConnectionString(lockName) ?? configuration[lockName];
            if (string.IsNullOrWhiteSpace(lockConnectionString))
            {
                throw new InvalidOperationException(
                    "SQL Server is the configured Kafka maintenance lock provider, but its explicit lock connection string is unavailable.");
            }

            _sqlLockDatabase = (lockName, lockConnectionString);
        }

        return;

        void ResolveConnectionStrings(string configuredNames, List<(string Name, string ConnStr)> target)
        {
            foreach (var connectionName in configuredNames.Split(
                         ',',
                         StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                var connectionString = configuration.GetConnectionString(connectionName) ?? configuration[connectionName];
                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    throw new InvalidOperationException(
                        $"Kafka maintenance connection string '{connectionName}' is unavailable.");
                }

                if (!target.Exists(item => string.Equals(item.Name, connectionName, StringComparison.Ordinal)))
                {
                    target.Add((connectionName, connectionString));
                }
            }
        }
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Kafka Maintenance Service started. Instance: {InstanceId}. Interval: {IntervalMinutes} minutes.",
            _instanceId,
            _options.Maintenance.IntervalMinutes);

        var interval = TimeSpan.FromMinutes(_options.Maintenance.IntervalMinutes);

        // Initial delay to let the service fully start before running maintenance
        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunMaintenanceCycleAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Service is shutting down
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    "Unhandled exception in Kafka Maintenance cycle. ErrorType: {ErrorType}.",
                    exception.GetType().Name);
            }

            try
            {
                await Task.Delay(interval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Service is shutting down
            }
        }

        _logger.LogInformation("Kafka Maintenance Service stopped.");
    }

    /// <summary>
    /// Runs a single maintenance cycle: acquires a distributed lock, performs cleanup, releases the lock.
    /// </summary>
    private async Task RunMaintenanceCycleAsync(CancellationToken cancellationToken)
    {
        var lockAcquired = _useRedisLock
            ? await TryAcquireRedisLockAsync()
            : await TryAcquireSqlLockAsync(cancellationToken);

        if (!lockAcquired)
        {
            _logger.LogDebug("Maintenance lock not acquired. Another instance is running maintenance. Skipping this cycle.");
            return;
        }

        try
        {
            _logger.LogInformation("Maintenance cycle started by instance {InstanceId}.", _instanceId);

            if (_useRedisLock)
            {
                await RunWithRedisLockRenewalAsync(cancellationToken);
            }
            else
            {
                await PerformMaintenanceWorkAsync(cancellationToken);
            }

            _logger.LogInformation("Maintenance cycle completed by instance {InstanceId}.", _instanceId);
        }
        finally
        {
            if (_useRedisLock)
            {
                await TryReleaseRedisLockAsync();
            }
            else
            {
                await TryReleaseSqlLockAsync(cancellationToken);
            }
        }
    }

    /// <summary>
    /// Attempts to acquire a distributed lock via Redis SETNX with TTL.
    /// </summary>
    /// <returns>True if the lock was acquired, false otherwise.</returns>
    private async Task<bool> TryAcquireRedisLockAsync()
    {
        try
        {
            if (!_redisConnectionResolver.IsRegistered())
            {
                return false;
            }

            var multiplexer = _redisConnectionResolver.Resolve();
            var database = multiplexer.GetDatabase();

            var wasSet = await database.StringSetAsync(
                LockKeyPrefix,
                _instanceId,
                expiry: LockTtl,
                when: When.NotExists);

            return wasSet;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                "Failed to acquire the configured Redis maintenance lock. This cycle will be skipped. ErrorType: {ErrorType}.",
                exception.GetType().Name);
            return false;
        }
    }

    /// <summary>
    /// Attempts to release the Redis distributed lock.
    /// Only releases if the lock is still owned by this instance (compare-and-delete via Lua).
    /// </summary>
    private async Task TryReleaseRedisLockAsync()
    {
        try
        {
            if (!_redisConnectionResolver.IsRegistered())
            {
                return;
            }

            var multiplexer = _redisConnectionResolver.Resolve();
            var database = multiplexer.GetDatabase();

            // Lua script: only delete if the value matches our instance ID
            const string releaseLuaScript = @"
                if redis.call('GET', KEYS[1]) == ARGV[1] then
                    return redis.call('DEL', KEYS[1])
                end
                return 0";

            await database.ScriptEvaluateAsync(
                releaseLuaScript,
                new RedisKey[] { LockKeyPrefix },
                new RedisValue[] { _instanceId });
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                "Failed to release Redis maintenance lock. It will expire via TTL. ErrorType: {ErrorType}.",
                exception.GetType().Name);
        }
    }

    /// <summary>
    /// Lock acquisition using SQL Server sp_getapplock when Redis was not registered at startup.
    /// </summary>
    /// <returns>True if the lock was acquired, false otherwise.</returns>
    private async Task<bool> TryAcquireSqlLockAsync(CancellationToken cancellationToken)
    {
        if (_sqlLockDatabase is null)
        {
            return false;
        }

        SqlConnection? connection = null;
        try
        {
            connection = new SqlConnection(_sqlLockDatabase.Value.ConnStr);
            await connection.OpenAsync(cancellationToken);

            var result = await connection.ExecuteScalarAsync<int>(
                new CommandDefinition(
                    SqlLockAcquireCommand,
                    new { Resource = LockKeyPrefix },
                    cancellationToken: cancellationToken));

            if (IsSqlLockAcquired(result))
            {
                _sqlLockConnection = connection;
                connection = null;
                return true;
            }

            await connection.DisposeAsync();
            connection = null;
            return false;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }
            throw;
        }
        catch (Exception exception)
        {
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }
            _logger.LogWarning(
                "Failed to acquire SQL Server application lock for maintenance on connection '{ConnectionName}'. ErrorType: {ErrorType}.",
                _sqlLockDatabase.Value.Name,
                exception.GetType().Name);
            return false;
        }
    }

    private async Task PerformMaintenanceWorkAsync(CancellationToken cancellationToken)
    {
        var failures = new List<Exception>();
        await ExecuteStepAsync(() => CleanupStaleAssembliesAsync(cancellationToken));
        await ExecuteStepAsync(() => CleanupCompletedJobsAsync(cancellationToken));
        await ExecuteStepAsync(() => CleanupPermanentlyFailedOutboxAsync(cancellationToken));

        if (_options.Maintenance.EnableIndexMaintenance)
        {
            await ExecuteStepAsync(() => PerformIndexMaintenanceIfNeededAsync(cancellationToken));
        }

        if (failures.Count > 0)
        {
            throw new AggregateException("Kafka maintenance cycle completed with partial failures.", failures);
        }

        return;

        async Task ExecuteStepAsync(Func<Task> step)
        {
            try
            {
                await step();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }
    }

    private async Task RunWithRedisLockRenewalAsync(CancellationToken cancellationToken)
    {
        using var cycleCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var maintenanceTask = PerformMaintenanceWorkAsync(cycleCancellation.Token);
        var renewalTask = RenewRedisLockUntilCancelledAsync(cycleCancellation.Token);
        var completedTask = await Task.WhenAny(maintenanceTask, renewalTask);

        if (completedTask == renewalTask)
        {
            cycleCancellation.Cancel();
            try
            {
                await maintenanceTask;
            }
            catch (OperationCanceledException) when (cycleCancellation.IsCancellationRequested)
            {
            }

            await renewalTask;
            throw new InvalidOperationException("Redis maintenance lock renewal stopped unexpectedly.");
        }

        cycleCancellation.Cancel();
        try
        {
            await renewalTask;
        }
        catch (OperationCanceledException) when (cycleCancellation.IsCancellationRequested)
        {
        }

        await maintenanceTask;
    }

    private async Task RenewRedisLockUntilCancelledAsync(CancellationToken cancellationToken)
    {
        var database = _redisConnectionResolver.Resolve().GetDatabase();
        var interval = TimeSpan.FromTicks(LockTtl.Ticks / 3);
        using var timer = new PeriodicTimer(interval);
        const string renewLuaScript = """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('PEXPIRE', KEYS[1], ARGV[2])
            end
            return 0
            """;

        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            var renewed = (int)(await database.ScriptEvaluateAsync(
                renewLuaScript,
                [new RedisKey(LockKeyPrefix)],
                [new RedisValue(_instanceId), new RedisValue(((long)LockTtl.TotalMilliseconds).ToString(System.Globalization.CultureInfo.InvariantCulture))]));

            if (renewed != 1)
            {
                throw new InvalidOperationException("Redis maintenance lock was lost.");
            }
        }
    }

    internal static bool IsSqlLockAcquired(int result) => result >= 0;

    private async Task TryReleaseSqlLockAsync(CancellationToken cancellationToken)
    {
        var connection = Interlocked.Exchange(ref _sqlLockConnection, null);
        if (connection is null)
        {
            return;
        }

        try
        {
            await connection.ExecuteAsync(new CommandDefinition(
                "EXEC sp_releaseapplock @Resource = @Resource, @LockOwner = 'Session';",
                new { Resource = LockKeyPrefix },
                cancellationToken: cancellationToken));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                "Failed to release SQL Server maintenance lock. ErrorType: {ErrorType}.",
                exception.GetType().Name);
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }

    /// <summary>
    /// Deletes stale pending_message_assemblies records that have exceeded the threshold.
    /// </summary>
    private async Task CleanupStaleAssembliesAsync(CancellationToken cancellationToken)
    {
        var failures = new List<Exception>();
        foreach (var (name, connStr) in _assemblyConnections)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await CleanupStaleAssembliesForConnectionAsync(name, connStr, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }

        if (failures.Count > 0) throw new AggregateException("Assembly cleanup partially failed.", failures);
    }

    private async Task CleanupStaleAssembliesForConnectionAsync(string connectionName, string connectionString, CancellationToken cancellationToken)
    {
        try
        {
            var assemblyOptions = _options.Assembly;
            var schema = SqlIdentifierValidator.ValidateIdentifier(assemblyOptions.TableSchema, nameof(assemblyOptions.TableSchema));
            var thresholdSeconds = _options.Maintenance.StaleAssemblyThresholdSeconds;

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Check if the table exists before attempting cleanup
            var tableExistsSql = $"SELECT OBJECT_ID('[{schema}].pending_message_assemblies', 'U');";
            var objectId = await connection.ExecuteScalarAsync<int?>(
                new CommandDefinition(tableExistsSql, cancellationToken: cancellationToken));

            if (!objectId.HasValue)
            {
                return;
            }

            var deleteSql = $@"
                DELETE FROM [{schema}].pending_message_assemblies
                WHERE created_at < DATEADD(SECOND, -@ThresholdSeconds, GETUTCDATE());";

            var deletedCount = await connection.ExecuteAsync(
                new CommandDefinition(
                    deleteSql,
                    new { ThresholdSeconds = thresholdSeconds },
                    cancellationToken: cancellationToken));

            if (deletedCount > 0)
            {
                _logger.LogInformation(
                    "Maintenance: Deleted {DeletedCount} stale message assembly segments older than {ThresholdSeconds} seconds on connection '{ConnectionName}'.",
                    deletedCount,
                    thresholdSeconds,
                    connectionName);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                "Maintenance: Error cleaning up stale message assemblies on connection '{ConnectionName}'. ErrorType: {ErrorType}.",
                connectionName,
                exception.GetType().Name);
            throw;
        }
    }

    /// <summary>
    /// Deletes completed staged_resolve_jobs older than the retention period.
    /// </summary>
    private async Task CleanupCompletedJobsAsync(CancellationToken cancellationToken)
    {
        var failures = new List<Exception>();
        foreach (var (name, connStr) in _stagedJobConnections)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await CleanupCompletedJobsForConnectionAsync(name, connStr, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }

        if (failures.Count > 0) throw new AggregateException("Staged-job cleanup partially failed.", failures);
    }

    private async Task CleanupCompletedJobsForConnectionAsync(string connectionName, string connectionString, CancellationToken cancellationToken)
    {
        try
        {
            var jobSettings = _options.StagedJobs;
            var schema = SqlIdentifierValidator.ValidateIdentifier(jobSettings.TableSchema, nameof(jobSettings.TableSchema));
            var retentionDays = _options.Maintenance.CompletedJobRetentionDays;

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Check if the table exists before attempting cleanup
            var tableExistsSql = $"SELECT OBJECT_ID('[{schema}].staged_resolve_jobs', 'U');";
            var objectId = await connection.ExecuteScalarAsync<int?>(
                new CommandDefinition(tableExistsSql, cancellationToken: cancellationToken));

            if (!objectId.HasValue)
            {
                return;
            }

            var deleteSql = $@"
                DELETE FROM [{schema}].staged_resolve_jobs
                WHERE status = 'Completed'
                  AND updated_at < DATEADD(DAY, -@RetentionDays, GETUTCDATE());";

            var deletedCount = await connection.ExecuteAsync(
                new CommandDefinition(
                    deleteSql,
                    new { RetentionDays = retentionDays },
                    cancellationToken: cancellationToken));

            if (deletedCount > 0)
            {
                _logger.LogInformation(
                    "Maintenance: Deleted {DeletedCount} completed staged jobs older than {RetentionDays} days on connection '{ConnectionName}'.",
                    deletedCount,
                    retentionDays,
                    connectionName);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                "Maintenance: Error cleaning up completed staged jobs on connection '{ConnectionName}'. ErrorType: {ErrorType}.",
                connectionName,
                exception.GetType().Name);
            throw;
        }
    }

    /// <summary>
    /// Deletes permanently failed outbox records older than the retention period.
    /// </summary>
    private async Task CleanupPermanentlyFailedOutboxAsync(CancellationToken cancellationToken)
    {
        var failures = new List<Exception>();
        foreach (var (name, connStr) in _outboxConnections)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await CleanupPermanentlyFailedOutboxForConnectionAsync(name, connStr, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }

        if (failures.Count > 0) throw new AggregateException("Outbox cleanup partially failed.", failures);
    }

    private async Task CleanupPermanentlyFailedOutboxForConnectionAsync(string connectionName, string connectionString, CancellationToken cancellationToken)
    {
        try
        {
            var outboxSettings = _options.Outbox;
            var schema = SqlIdentifierValidator.ValidateIdentifier(outboxSettings.TableSchema, nameof(outboxSettings.TableSchema));
            var retentionDays = _options.Maintenance.PermanentlyFailedOutboxRetentionDays;

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Check if the table exists before attempting cleanup
            var tableExistsSql = $"SELECT OBJECT_ID('[{schema}].kafka_outbox', 'U');";
            var objectId = await connection.ExecuteScalarAsync<int?>(
                new CommandDefinition(tableExistsSql, cancellationToken: cancellationToken));

            if (!objectId.HasValue)
            {
                return;
            }

            var deleteSql = $@"
                DELETE FROM [{schema}].kafka_outbox
                WHERE processing_status = 'PermanentlyFailed'
                  AND updated_at < DATEADD(DAY, -@RetentionDays, GETUTCDATE());";

            var deletedCount = await connection.ExecuteAsync(
                new CommandDefinition(
                    deleteSql,
                    new { RetentionDays = retentionDays },
                    cancellationToken: cancellationToken));

            if (deletedCount > 0)
            {
                _logger.LogInformation(
                    "Maintenance: Deleted {DeletedCount} permanently failed outbox records older than {RetentionDays} days on connection '{ConnectionName}'.",
                    deletedCount,
                    retentionDays,
                    connectionName);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                "Maintenance: Error cleaning up permanently failed outbox records on connection '{ConnectionName}'. ErrorType: {ErrorType}.",
                connectionName,
                exception.GetType().Name);
            throw;
        }
    }

    /// <summary>
    /// Checks if weekly index maintenance is due and runs index defragmentation for active tables.
    /// </summary>
    private async Task PerformIndexMaintenanceIfNeededAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var interval = TimeSpan.FromHours(_options.Maintenance.IndexMaintenanceIntervalHours);
        var shouldRun = _useRedisLock
            ? await ShouldRunRedisIndexMaintenanceAsync(now, interval)
            : await ShouldRunSqlIndexMaintenanceAsync(now, interval, cancellationToken);

        if (!shouldRun)
        {
            return;
        }

        _logger.LogInformation("Maintenance: Index maintenance starting.");

        if (_options.Assembly.IsEnabled && _options.Assembly.Provider == AssemblyProvider.SqlServer)
        {
            await RebuildIndexesForTableAsync(
                _options.Assembly.ConnectionStringName,
                _options.Assembly.TableSchema,
                "pending_message_assemblies",
                cancellationToken);
        }

        if (_options.StagedJobs.IsEnabled)
        {
            await RebuildIndexesForTableAsync(
                _options.StagedJobs.ConnectionStringName,
                _options.StagedJobs.TableSchema,
                "staged_resolve_jobs",
                cancellationToken);
        }

        if (_options.Outbox.IsEnabled)
        {
            await RebuildIndexesForTableAsync(
                _options.Outbox.ConnectionStringName,
                _options.Outbox.TableSchema,
                "kafka_outbox",
                cancellationToken);
        }

        if (_useRedisLock)
        {
            var database = _redisConnectionResolver.Resolve().GetDatabase();
            await database.StringSetAsync(
                LastIndexMaintenanceKey,
                now.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture));
        }
        else
        {
            await SaveSqlIndexMaintenanceTimeAsync(now, cancellationToken);
        }

        _logger.LogInformation("Maintenance: Index maintenance completed successfully.");
    }

    private async Task<bool> ShouldRunRedisIndexMaintenanceAsync(DateTimeOffset now, TimeSpan interval)
    {
        var database = _redisConnectionResolver.Resolve().GetDatabase();
        var lastRunValue = await database.StringGetAsync(LastIndexMaintenanceKey);
        if (!lastRunValue.HasValue)
        {
            return true;
        }

        if (!long.TryParse(lastRunValue.ToString(), NumberStyles.None, CultureInfo.InvariantCulture, out var unixTimeSeconds))
        {
            throw new InvalidOperationException("Redis index-maintenance marker is invalid.");
        }

        DateTimeOffset lastRun;
        try
        {
            lastRun = DateTimeOffset.FromUnixTimeSeconds(unixTimeSeconds);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new InvalidOperationException("Redis index-maintenance marker is outside the supported timestamp range.", exception);
        }

        return now - lastRun >= interval;
    }

    private async Task<bool> ShouldRunSqlIndexMaintenanceAsync(
        DateTimeOffset now,
        TimeSpan interval,
        CancellationToken cancellationToken)
    {
        var connection = _sqlLockConnection
            ?? throw new InvalidOperationException("SQL maintenance lock connection is unavailable.");
        await EnsureSqlMaintenanceStateTableAsync(connection, cancellationToken);
        var lastRun = await connection.QuerySingleOrDefaultAsync<DateTime?>(new CommandDefinition(
            "SELECT last_run_utc FROM dbo.kafka_maintenance_state WHERE state_key = @StateKey;",
            new { StateKey = LastIndexMaintenanceKey },
            commandTimeout: _options.Maintenance.IndexCommandTimeoutSeconds,
            cancellationToken: cancellationToken));
        return !lastRun.HasValue || now.UtcDateTime - DateTime.SpecifyKind(lastRun.Value, DateTimeKind.Utc) >= interval;
    }

    private async Task SaveSqlIndexMaintenanceTimeAsync(
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var connection = _sqlLockConnection
            ?? throw new InvalidOperationException("SQL maintenance lock connection is unavailable.");
        await EnsureSqlMaintenanceStateTableAsync(connection, cancellationToken);
        const string upsertSql = """
            UPDATE dbo.kafka_maintenance_state
            SET last_run_utc = @LastRunUtc
            WHERE state_key = @StateKey;
            IF @@ROWCOUNT = 0
                INSERT INTO dbo.kafka_maintenance_state (state_key, last_run_utc)
                VALUES (@StateKey, @LastRunUtc);
            """;
        await connection.ExecuteAsync(new CommandDefinition(
            upsertSql,
            new { StateKey = LastIndexMaintenanceKey, LastRunUtc = now.UtcDateTime },
            commandTimeout: _options.Maintenance.IndexCommandTimeoutSeconds,
            cancellationToken: cancellationToken));
    }

    private async Task EnsureSqlMaintenanceStateTableAsync(
        SqlConnection connection,
        CancellationToken cancellationToken)
    {
        const string ddl = """
            IF OBJECT_ID(N'dbo.kafka_maintenance_state', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.kafka_maintenance_state
                (
                    state_key nvarchar(128) NOT NULL PRIMARY KEY,
                    last_run_utc datetime2(7) NOT NULL
                );
            END;
            """;
        await connection.ExecuteAsync(new CommandDefinition(
            ddl,
            commandTimeout: _options.Maintenance.IndexCommandTimeoutSeconds,
            cancellationToken: cancellationToken));
    }

    private async Task RebuildIndexesForTableAsync(
        string connectionStringName, 
        string tableSchema, 
        string tableName, 
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(connectionStringName))
        {
            return;
        }

        var connectionNames = connectionStringName.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var name in connectionNames)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var connectionString = _configuration.GetConnectionString(name) ?? _configuration[name];
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                continue;
            }

            await RebuildIndexesForTableConnectionAsync(name, connectionString, tableSchema, tableName, cancellationToken);
        }
    }

    private async Task RebuildIndexesForTableConnectionAsync(
        string connectionName,
        string connectionString,
        string tableSchema, 
        string tableName, 
        CancellationToken cancellationToken)
    {
        try
        {
            var schema = SqlIdentifierValidator.ValidateIdentifier(tableSchema, nameof(tableSchema));
            var validatedTableName = SqlIdentifierValidator.ValidateIdentifier(tableName, nameof(tableName));

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Check if the table exists before attempting rebuild
            var tableExistsSql = $"SELECT OBJECT_ID('[{schema}].[{validatedTableName}]', 'U');";
            var objectId = await connection.ExecuteScalarAsync<int?>(
                new CommandDefinition(tableExistsSql, cancellationToken: cancellationToken));

            if (!objectId.HasValue)
            {
                _logger.LogDebug("Maintenance: Table [{Schema}].[{TableName}] does not exist on connection '{ConnectionName}'. Skipping index rebuild.", schema, validatedTableName, connectionName);
                return;
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
                _logger.LogInformation(
                    "Maintenance: Rebuilding fragmented index [{IndexName}] for [{Schema}].[{TableName}] on connection '{ConnectionName}'.",
                    validatedIndexName,
                    schema,
                    validatedTableName,
                    connectionName);
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
                "Maintenance: SQL error rebuilding indexes for table [{Schema}].[{TableName}] on connection '{ConnectionName}'. ErrorType: {ErrorType}.",
                tableSchema,
                tableName,
                connectionName,
                exception.GetType().Name);
            throw;
        }
    }
}
