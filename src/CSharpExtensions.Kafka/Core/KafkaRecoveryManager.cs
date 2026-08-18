using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

internal interface IKafkaRecoveryLease : IAsyncDisposable
{
    CancellationToken LeaseLostToken { get; }

    bool IsLost { get; }

    void ThrowIfLost();
}

internal interface IKafkaRecoveryLockProvider
{
    Task<IKafkaRecoveryLease> AcquireAsync(
        string connectionString,
        IEnumerable<string> protectedTopicNames,
        int lockTimeoutMs,
        CancellationToken cancellationToken);
}

/// <summary>
/// Manages the orchestration and execution of background topic recovery and schema upcasting processes.
/// Kept as a Singleton to prevent unmanaged task leaks and to track progress safely.
/// </summary>
public sealed class KafkaRecoveryManager : IDisposable, IAsyncDisposable
{
    private readonly IDbStagedRepairPipeline _repairPipeline;
    private readonly IEnumerable<KafkaRepairConfiguration> _repairConfigurations;
    private readonly KafkaOptions _options;
    private readonly IConfiguration _configuration;
    private readonly ILogger<KafkaRecoveryManager> _logger;
    private readonly IKafkaRecoveryLockProvider _recoveryLockProvider;

    private readonly ConcurrentDictionary<string, KafkaRecoveryStatus> _statuses = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _cts = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, Task> _jobs = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _lifecycleGate = new();
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaRecoveryManager"/> class.
    /// </summary>
    /// <param name="repairPipeline">The staged repair pipeline.</param>
    /// <param name="repairConfigurations">The registered repair configurations.</param>
    /// <param name="options">The global Kafka options.</param>
    /// <param name="configuration">The configuration service.</param>
    /// <param name="logger">The logger instance.</param>
    public KafkaRecoveryManager(
        IDbStagedRepairPipeline repairPipeline,
        IEnumerable<KafkaRepairConfiguration> repairConfigurations,
        IOptions<KafkaOptions> options,
        IConfiguration configuration,
        ILogger<KafkaRecoveryManager> logger)
        : this(
            repairPipeline,
            repairConfigurations,
            options,
            configuration,
            logger,
            new SqlServerRecoveryLockProvider())
    {
    }

    internal KafkaRecoveryManager(
        IDbStagedRepairPipeline repairPipeline,
        IEnumerable<KafkaRepairConfiguration> repairConfigurations,
        IOptions<KafkaOptions> options,
        IConfiguration configuration,
        ILogger<KafkaRecoveryManager> logger,
        IKafkaRecoveryLockProvider recoveryLockProvider)
    {
        _repairPipeline = repairPipeline ?? throw new ArgumentNullException(nameof(repairPipeline));
        _repairConfigurations = repairConfigurations?.ToArray() ?? throw new ArgumentNullException(nameof(repairConfigurations));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _recoveryLockProvider = recoveryLockProvider ?? throw new ArgumentNullException(nameof(recoveryLockProvider));

        // Initialize statuses for all registered repairs
        foreach (var config in _repairConfigurations)
        {
            ValidateRepairConfiguration(config);
            var topicName = ResolveTopicName(config);
            _statuses[topicName] = new KafkaRecoveryStatus
            {
                TopicName = topicName,
                Phase = "Pending",
                ProcessedCount = 0,
                TotalCount = 0
            };
        }
    }

    /// <summary>
    /// Gets the current status of all registered topic recovery processes.
    /// </summary>
    /// <returns>A list of recovery statuses.</returns>
    public IReadOnlyCollection<KafkaRecoveryStatus> GetStatuses()
    {
        return _statuses.Values
            .Select(status => new KafkaRecoveryStatus
            {
                TopicName = status.TopicName,
                Phase = status.Phase,
                ProcessedCount = status.ProcessedCount,
                TotalCount = status.TotalCount,
                ErrorMessage = status.ErrorMessage
            })
            .ToList()
            .AsReadOnly();
    }

    /// <summary>
    /// Starts background recovery tasks for all registered topic repair configurations.
    /// </summary>
    /// <returns>A Result indicating whether the startup succeeded.</returns>
    public Result StartAllRecoveries()
    {
        if (IsDisposed())
        {
            return Result.Failure("Cannot start recoveries. The recovery manager has been disposed.");
        }

        if (!_repairConfigurations.Any())
        {
            return Result.Failure("No topic repairs are registered. Use kafka.UseTopicRepair<T>() to configure topic repairs.");
        }

        foreach (var config in _repairConfigurations)
        {
            var topicName = ResolveTopicName(config);
            TryStartRecoveryJob(config, topicName);
        }

        return Result.Success();
    }

    /// <summary>
    /// Starts background recovery for a specific topic configuration, extracting messages
    /// from an alternative source topic, upcasting them, and publishing to the target topic.
    /// </summary>
    public Result StartRecoveryFromSource(string topicConfigurationKey, string sourceTopicName)
    {
        if (IsDisposed())
        {
            return Result.Failure("Cannot start recovery. The recovery manager has been disposed.");
        }

        if (string.IsNullOrWhiteSpace(topicConfigurationKey)) throw new ArgumentException("Topic configuration key cannot be empty.", nameof(topicConfigurationKey));
        if (!IsSafeRecoveryTopicName(sourceTopicName))
        {
            return Result.Failure("The source Kafka topic name is invalid.");
        }

        var config = _repairConfigurations.FirstOrDefault(c => string.Equals(c.TopicConfigurationKey, topicConfigurationKey, StringComparison.OrdinalIgnoreCase));
        if (config == null)
        {
            return Result.Failure("Kafka topic repair configuration is not registered.");
        }

        var targetTopicName = ResolveTopicName(config);
        
        if (!TryStartRecoveryJobFromSource(config, sourceTopicName, targetTopicName))
        {
            return Result.Failure("A Kafka recovery process is already active.");
        }

        return Result.Success();
    }

    private bool TryStartRecoveryJobFromSource(KafkaRepairConfiguration config, string sourceTopicName, string targetTopicName)
    {
        return TryStartTrackedRecovery(targetTopicName, async cancellationToken =>
        {
            var status = _statuses.GetOrAdd(targetTopicName, name => new KafkaRecoveryStatus { TopicName = name });
            status.Phase = "Pending";
            status.ProcessedCount = 0;
            status.TotalCount = 0;
            status.ErrorMessage = null;
            IKafkaRecoveryLease? recoveryLease = null;

            try
            {
                var connectionStringName = config.Settings.ConnectionStringName;
                var connectionString = _configuration.GetConnectionString(connectionStringName) ?? _configuration[connectionStringName];

                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "The recovery database connection is unavailable.";
                    return;
                }

                recoveryLease = await _recoveryLockProvider.AcquireAsync(
                    connectionString,
                    [sourceTopicName, targetTopicName],
                    config.Settings.DistributedLockTimeoutMs,
                    cancellationToken);
                using var recoveryCancellation = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken,
                    recoveryLease.LeaseLostToken);
                var recoveryToken = recoveryCancellation.Token;

                // Phase 0: Clear staging tables to prevent offset conflicts
                status.Phase = "ClearingStaging";
                await _repairPipeline.TruncateStagingTableAsync(sourceTopicName, connectionString, recoveryToken);
                if (!string.Equals(sourceTopicName, targetTopicName, StringComparison.OrdinalIgnoreCase))
                {
                    await _repairPipeline.TruncateStagingTableAsync(targetTopicName, connectionString, recoveryToken);
                }

                recoveryToken.ThrowIfCancellationRequested();

                // Phase 1: Export legacy source topic to staging
                status.Phase = "Exporting";
                var exportResult = await _repairPipeline.ExportToStagingAsync(
                    sourceTopicName,
                    connectionString,
                    (processed, total) =>
                    {
                        status.ProcessedCount = processed;
                        status.TotalCount = total;
                    },
                    recoveryToken);

                if (!exportResult.IsSuccess)
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "Recovery export phase failed.";
                    return;
                }

                recoveryToken.ThrowIfCancellationRequested();

                // Phase 2: Copy from source staging schema to target staging schema if they differ
                if (!string.Equals(sourceTopicName, targetTopicName, StringComparison.OrdinalIgnoreCase))
                {
                    status.Phase = "CopyingStaging";
                    var copyResult = await _repairPipeline.CopyStagedRecordsAsync(sourceTopicName, targetTopicName, connectionString, recoveryToken);
                    if (!copyResult.IsSuccess)
                    {
                        status.Phase = "Failed";
                        status.ErrorMessage = "Recovery staging phase failed.";
                        return;
                    }
                }

                recoveryToken.ThrowIfCancellationRequested();

                // Phase 3: Upcast staged payloads
                status.Phase = "Upcasting";
                status.ProcessedCount = 0;
                status.TotalCount = 0;

                var upcastMethod = typeof(IDbStagedRepairPipeline)
                    .GetMethod(nameof(IDbStagedRepairPipeline.UpcastStagedPayloadsAsync))
                    ?.MakeGenericMethod(config.TargetMessageType);

                if (upcastMethod == null)
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "Failed to resolve UpcastStagedPayloadsAsync method.";
                    return;
                }

                var upcastProgressReporter = new Action<long, long>((processed, total) =>
                {
                    status.ProcessedCount = processed;
                    status.TotalCount = total;
                });

                var upcastTask = (Task<Result>?)upcastMethod.Invoke(_repairPipeline, new object?[]
                {
                    connectionString,
                    config.Settings.UpcastBatchSize,
                    upcastProgressReporter,
                    recoveryToken
                });

                if (upcastTask == null)
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "Failed to invoke UpcastStagedPayloadsAsync method.";
                    return;
                }

                var upcastResult = await upcastTask;
                if (!upcastResult.IsSuccess)
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "Recovery upcast phase failed.";
                    return;
                }

                recoveryToken.ThrowIfCancellationRequested();

                // Phase 4: Repopulate target topic
                status.Phase = "Repopulating";
                status.ProcessedCount = 0;
                status.TotalCount = 0;

                var repopulateResult = await _repairPipeline.RepopulateTopicFromStagingAsync(
                    targetTopicName,
                    connectionString,
                    config.Settings.RepopulateBatchSize,
                    (processed, total) =>
                    {
                        status.ProcessedCount = processed;
                        status.TotalCount = total;
                    },
                    recoveryToken);

                if (!repopulateResult.IsSuccess)
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "Recovery repopulation phase failed.";
                    return;
                }

                // Final cleanup of staging tables
                status.Phase = "ClearingStaging";
                await _repairPipeline.TruncateStagingTableAsync(sourceTopicName, connectionString, recoveryToken);
                if (!string.Equals(sourceTopicName, targetTopicName, StringComparison.OrdinalIgnoreCase))
                {
                    await _repairPipeline.TruncateStagingTableAsync(targetTopicName, connectionString, recoveryToken);
                }

                recoveryLease.ThrowIfLost();
                status.Phase = "Completed";
            }
            catch (RecoveryLockUnavailableException)
            {
                status.Phase = "Failed";
                status.ErrorMessage = "Another recovery process owns the required staging resources.";
            }
            catch (OperationCanceledException) when (recoveryLease?.IsLost == true)
            {
                status.Phase = "Failed";
                status.ErrorMessage = "Recovery stopped because its distributed ownership lock was lost.";
            }
            catch (OperationCanceledException)
            {
                status.Phase = "Failed";
                status.ErrorMessage = "Recovery process was cancelled.";
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    "Unhandled error during source recovery process. ErrorType: {ErrorType}.",
                    exception.GetType().Name);
                status.Phase = "Failed";
                status.ErrorMessage = "Recovery failed with an unexpected internal error.";
            }
            finally
            {
                if (recoveryLease is not null)
                {
                    await recoveryLease.DisposeAsync();
                }
            }
        });
    }

    private bool TryStartRecoveryJob(KafkaRepairConfiguration config, string topicName)
    {
        return TryStartTrackedRecovery(topicName, async cancellationToken =>
        {
            var status = _statuses.GetOrAdd(topicName, name => new KafkaRecoveryStatus { TopicName = name });
            status.Phase = "Pending";
            status.ProcessedCount = 0;
            status.TotalCount = 0;
            status.ErrorMessage = null;
            IKafkaRecoveryLease? recoveryLease = null;

            try
            {
                var connectionStringName = config.Settings.ConnectionStringName;
                var connectionString = _configuration.GetConnectionString(connectionStringName) ?? _configuration[connectionStringName];

                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "The recovery database connection is unavailable.";
                    return;
                }

                recoveryLease = await _recoveryLockProvider.AcquireAsync(
                    connectionString,
                    [topicName],
                    config.Settings.DistributedLockTimeoutMs,
                    cancellationToken);
                using var recoveryCancellation = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken,
                    recoveryLease.LeaseLostToken);
                var recoveryToken = recoveryCancellation.Token;

                // Phase 1: Export to staging
                status.Phase = "Exporting";
                var exportResult = await _repairPipeline.ExportToStagingAsync(
                    topicName,
                    connectionString,
                    (processed, total) =>
                    {
                        status.ProcessedCount = processed;
                        status.TotalCount = total;
                    },
                    recoveryToken);

                if (!exportResult.IsSuccess)
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "Recovery export phase failed.";
                    return;
                }

                recoveryToken.ThrowIfCancellationRequested();

                // Phase 2: Upcast staged payloads
                status.Phase = "Upcasting";
                status.ProcessedCount = 0;
                status.TotalCount = 0;

                var upcastMethod = typeof(IDbStagedRepairPipeline)
                    .GetMethod(nameof(IDbStagedRepairPipeline.UpcastStagedPayloadsAsync))
                    ?.MakeGenericMethod(config.TargetMessageType);

                if (upcastMethod == null)
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "Failed to resolve UpcastStagedPayloadsAsync method.";
                    return;
                }

                var upcastProgressReporter = new Action<long, long>((processed, total) =>
                {
                    status.ProcessedCount = processed;
                    status.TotalCount = total;
                });

                var upcastTask = (Task<Result>?)upcastMethod.Invoke(_repairPipeline, new object?[]
                {
                    connectionString,
                    config.Settings.UpcastBatchSize,
                    upcastProgressReporter,
                    recoveryToken
                });

                if (upcastTask == null)
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "Failed to invoke UpcastStagedPayloadsAsync method.";
                    return;
                }

                var upcastResult = await upcastTask;
                if (!upcastResult.IsSuccess)
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "Recovery upcast phase failed.";
                    return;
                }

                recoveryToken.ThrowIfCancellationRequested();

                // Phase 3: Repopulate target topic
                status.Phase = "Repopulating";
                status.ProcessedCount = 0;
                status.TotalCount = 0;

                var repopulateResult = await _repairPipeline.RepopulateTopicFromStagingAsync(
                    topicName,
                    connectionString,
                    config.Settings.RepopulateBatchSize,
                    (processed, total) =>
                    {
                        status.ProcessedCount = processed;
                        status.TotalCount = total;
                    },
                    recoveryToken);

                if (!repopulateResult.IsSuccess)
                {
                    status.Phase = "Failed";
                    status.ErrorMessage = "Recovery repopulation phase failed.";
                    return;
                }

                recoveryLease.ThrowIfLost();
                status.Phase = "Completed";
            }
            catch (RecoveryLockUnavailableException)
            {
                status.Phase = "Failed";
                status.ErrorMessage = "Another recovery process owns the required staging resources.";
            }
            catch (OperationCanceledException) when (recoveryLease?.IsLost == true)
            {
                status.Phase = "Failed";
                status.ErrorMessage = "Recovery stopped because its distributed ownership lock was lost.";
            }
            catch (OperationCanceledException)
            {
                status.Phase = "Failed";
                status.ErrorMessage = "Recovery process was cancelled.";
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    "Unhandled error during recovery process. ErrorType: {ErrorType}.",
                    exception.GetType().Name);
                status.Phase = "Failed";
                status.ErrorMessage = "Recovery failed with an unexpected internal error.";
            }
            finally
            {
                if (recoveryLease is not null)
                {
                    await recoveryLease.DisposeAsync();
                }
            }
        });
    }

    private bool TryStartTrackedRecovery(
        string recoveryKey,
        Func<CancellationToken, Task> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);

        var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationTokenSource cancellationSource;

        lock (_lifecycleGate)
        {
            if (_disposed || _cts.ContainsKey(recoveryKey))
            {
                return false;
            }

            cancellationSource = new CancellationTokenSource();
            if (!_cts.TryAdd(recoveryKey, cancellationSource))
            {
                cancellationSource.Dispose();
                return false;
            }

            var task = ExecuteTrackedRecoveryAsync(
                recoveryKey,
                cancellationSource,
                startGate.Task,
                operation);
            if (!_jobs.TryAdd(recoveryKey, task))
            {
                _cts.TryRemove(recoveryKey, out _);
                startGate.SetCanceled();
                cancellationSource.Dispose();
                return false;
            }

            startGate.SetResult();
        }

        return true;
    }

    private async Task ExecuteTrackedRecoveryAsync(
        string recoveryKey,
        CancellationTokenSource cancellationSource,
        Task startGate,
        Func<CancellationToken, Task> operation)
    {
        await startGate.ConfigureAwait(false);

        try
        {
            await operation(cancellationSource.Token).ConfigureAwait(false);
        }
        finally
        {
            lock (_lifecycleGate)
            {
                _jobs.TryRemove(recoveryKey, out _);
                _cts.TryRemove(recoveryKey, out _);
            }

            cancellationSource.Dispose();
        }
    }

    private static void ValidateRepairConfiguration(KafkaRepairConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var settings = configuration.Settings;
        if (string.IsNullOrWhiteSpace(settings.ConnectionStringName)
            || settings.ConnectionStringName.Length > 128
            || settings.ConnectionStringName.Any(char.IsControl))
        {
            throw new InvalidOperationException("A Kafka repair connection string name is invalid.");
        }

        SqlIdentifierValidator.ValidateIdentifier(settings.TableSchema, nameof(settings.TableSchema));
        if (settings.ExportBatchSize is < 1 or > 10000
            || settings.UpcastBatchSize is < 1 or > 10000
            || settings.RepopulateBatchSize is < 1 or > 10000
            || settings.DistributedLockTimeoutMs is < 1000 or > 300000)
        {
            throw new InvalidOperationException("Kafka repair batch or distributed lock settings are outside supported bounds.");
        }
    }

    private bool IsDisposed()
    {
        lock (_lifecycleGate)
        {
            return _disposed;
        }
    }

    /// <summary>
    /// Waits for the recoveries that are active at the time of the call to finish.
    /// </summary>
    public async Task WaitForActiveRecoveriesAsync(CancellationToken cancellationToken = default)
    {
        Task[] jobs;
        lock (_lifecycleGate)
        {
            jobs = _jobs.Values.ToArray();
        }

        if (jobs.Length > 0)
        {
            await Task.WhenAll(jobs).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static bool IsSafeRecoveryTopicName(string? topicName)
    {
        if (string.IsNullOrWhiteSpace(topicName)
            || topicName.Length > 249
            || topicName is "." or "..")
        {
            return false;
        }

        foreach (var character in topicName)
        {
            if (!char.IsAsciiLetterOrDigit(character)
                && character is not '.' and not '_' and not '-')
            {
                return false;
            }
        }

        return true;
    }

    private sealed class RecoveryLockUnavailableException : Exception
    {
    }

    private sealed class SqlServerRecoveryLockProvider : IKafkaRecoveryLockProvider
    {
        public async Task<IKafkaRecoveryLease> AcquireAsync(
            string connectionString,
            IEnumerable<string> protectedTopicNames,
            int lockTimeoutMs,
            CancellationToken cancellationToken)
        {
            return await RecoveryLockLease.AcquireAsync(
                connectionString,
                protectedTopicNames,
                lockTimeoutMs,
                cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class RecoveryLockLease : IKafkaRecoveryLease
    {
        private const int MonitorIntervalMilliseconds = 1000;
        private const int MonitorCommandTimeoutSeconds = 5;
        private readonly SqlConnection _connection;
        private readonly string[] _resources;
        private readonly CancellationTokenSource _monitorCancellation = new();
        private readonly CancellationTokenSource _leaseLost = new();
        private readonly Task _monitorTask;
        private int _lost;
        private int _disposed;

        private RecoveryLockLease(SqlConnection connection, string[] resources)
        {
            _connection = connection;
            _resources = resources;
            _connection.StateChange += OnConnectionStateChanged;
            _monitorTask = MonitorAsync();
        }

        public CancellationToken LeaseLostToken => _leaseLost.Token;

        public bool IsLost => Volatile.Read(ref _lost) != 0;

        public static async Task<RecoveryLockLease> AcquireAsync(
            string connectionString,
            IEnumerable<string> protectedTopicNames,
            int lockTimeoutMs,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new InvalidOperationException("A recovery database connection is required for distributed ownership.");
            }

            if (lockTimeoutMs is < 1000 or > 300000)
            {
                throw new InvalidOperationException("The recovery distributed lock timeout is outside supported bounds.");
            }

            var resources = protectedTopicNames
                .Select(CreateOpaqueLockResource)
                .Distinct(StringComparer.Ordinal)
                .OrderBy(resource => resource, StringComparer.Ordinal)
                .ToArray();
            if (resources.Length == 0)
            {
                throw new InvalidOperationException("At least one recovery staging resource is required.");
            }

            var connection = new SqlConnection(connectionString);
            try
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                var commandTimeoutSeconds = Math.Clamp((lockTimeoutMs + 999) / 1000 + 5, 6, 305);
                foreach (var resource in resources)
                {
                    const string acquireSql = """
                        DECLARE @LockResult int;
                        EXEC @LockResult = sys.sp_getapplock
                            @Resource = @Resource,
                            @LockMode = 'Exclusive',
                            @LockOwner = 'Session',
                            @LockTimeout = @LockTimeout;
                        SELECT @LockResult;
                        """;
                    var lockResult = await connection.ExecuteScalarAsync<int>(new CommandDefinition(
                        acquireSql,
                        new { Resource = resource, LockTimeout = lockTimeoutMs },
                        commandTimeout: commandTimeoutSeconds,
                        cancellationToken: cancellationToken)).ConfigureAwait(false);
                    if (lockResult < 0)
                    {
                        throw new RecoveryLockUnavailableException();
                    }
                }

                return new RecoveryLockLease(connection, resources);
            }
            catch
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        public void ThrowIfLost()
        {
            if (IsLost)
            {
                throw new OperationCanceledException("The recovery distributed ownership lock was lost.", LeaseLostToken);
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _monitorCancellation.Cancel();
            try
            {
                await _monitorTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_monitorCancellation.IsCancellationRequested)
            {
            }

            _connection.StateChange -= OnConnectionStateChanged;
            if (_connection.State == ConnectionState.Open)
            {
                foreach (var resource in _resources.Reverse())
                {
                    try
                    {
                        const string releaseSql = """
                            DECLARE @ReleaseResult int;
                            EXEC @ReleaseResult = sys.sp_releaseapplock
                                @Resource = @Resource,
                                @LockOwner = 'Session';
                            SELECT @ReleaseResult;
                            """;
                        await _connection.ExecuteScalarAsync<int>(new CommandDefinition(
                            releaseSql,
                            new { Resource = resource },
                            commandTimeout: MonitorCommandTimeoutSeconds,
                            cancellationToken: CancellationToken.None)).ConfigureAwait(false);
                    }
                    catch
                    {
                        break;
                    }
                }
            }

            await _connection.DisposeAsync().ConfigureAwait(false);
            _monitorCancellation.Dispose();
            _leaseLost.Dispose();
        }

        private async Task MonitorAsync()
        {
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(MonitorIntervalMilliseconds));
            try
            {
                while (await timer.WaitForNextTickAsync(_monitorCancellation.Token).ConfigureAwait(false))
                {
                    foreach (var resource in _resources)
                    {
                        const string verifySql =
                            "SELECT APPLOCK_MODE('public', @Resource, 'Session');";
                        var mode = await _connection.ExecuteScalarAsync<string>(new CommandDefinition(
                            verifySql,
                            new { Resource = resource },
                            commandTimeout: MonitorCommandTimeoutSeconds,
                            cancellationToken: _monitorCancellation.Token)).ConfigureAwait(false);
                        if (!string.Equals(mode, "Exclusive", StringComparison.Ordinal))
                        {
                            MarkLost();
                            return;
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (_monitorCancellation.IsCancellationRequested)
            {
            }
            catch
            {
                MarkLost();
            }
        }

        private static string CreateOpaqueLockResource(string topicName)
        {
            if (!IsSafeRecoveryTopicName(topicName))
            {
                throw new InvalidOperationException("A recovery topic name is invalid.");
            }

            var encoded = Encoding.UTF8.GetBytes(topicName);
            Span<byte> hash = stackalloc byte[32];
            SHA256.HashData(encoded, hash);
            return $"csharpextensions.kafka.recovery.{Convert.ToHexString(hash)}";
        }

        private void OnConnectionStateChanged(object sender, StateChangeEventArgs args)
        {
            if (Volatile.Read(ref _disposed) == 0 && args.CurrentState != ConnectionState.Open)
            {
                MarkLost();
            }
        }

        private void MarkLost()
        {
            if (Interlocked.Exchange(ref _lost, 1) == 0)
            {
                try
                {
                    _leaseLost.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }
    }

    /// <summary>
    /// Resolves the physical topic name for a given configuration key.
    /// </summary>
    public string ResolveTopicName(string topicConfigurationKey)
    {
        var config = _repairConfigurations.FirstOrDefault(c => string.Equals(c.TopicConfigurationKey, topicConfigurationKey, StringComparison.OrdinalIgnoreCase));
        if (config == null) return topicConfigurationKey;
        return ResolveTopicName(config);
    }

    private string ResolveTopicName(KafkaRepairConfiguration config)
    {
        var topicConfig = _options.Topics.TryGetValue(config.TopicConfigurationKey, out var tc) ? tc : null;
        return topicConfig?.TopicName ?? config.TopicConfigurationKey;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        CancelActiveRecoveries();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        var jobs = CancelActiveRecoveries();
        if (jobs.Length > 0)
        {
            await Task.WhenAll(jobs).ConfigureAwait(false);
        }
    }

    private Task[] CancelActiveRecoveries()
    {
        Task[] jobs;
        CancellationTokenSource[] cancellationSources;

        lock (_lifecycleGate)
        {
            _disposed = true;
            jobs = _jobs.Values.ToArray();
            cancellationSources = _cts.Values.ToArray();
        }

        foreach (var cancellationSource in cancellationSources)
        {
            try
            {
                cancellationSource.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // A completed job won the cleanup race.
            }
        }

        return jobs;
    }
}
