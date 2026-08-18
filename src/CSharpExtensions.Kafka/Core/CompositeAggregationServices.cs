using CSharpExtensions.Core.Helpers;
using CSharpExtensions.Core.Json;
using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Registry holding all configured composite aggregators.
/// </summary>
public sealed class CompositeMessageRegistry
{
    private readonly Dictionary<Type, object> _builders = new();

    /// <summary>
    /// Registers a composite aggregation configuration.
    /// </summary>
    public void Register<TComposite>(CompositeMessageBuilder<TComposite> builder)
        where TComposite : class, ICompositeContext
    {
        _builders[typeof(TComposite)] = builder;
    }

    /// <summary>
    /// Retrieves a composite aggregator builder configuration by composite type.
    /// </summary>
    public CompositeMessageBuilder<TComposite>? GetBuilder<TComposite>()
        where TComposite : class, ICompositeContext
    {
        if (_builders.TryGetValue(typeof(TComposite), out var builder))
        {
            return (CompositeMessageBuilder<TComposite>)builder;
        }
        return null;
    }

    /// <summary>
    /// Gets the builder configuration object for the specified composite type.
    /// </summary>
    public object? GetBuilder(Type compositeType)
    {
        _builders.TryGetValue(compositeType, out var builder);
        return builder;
    }

    /// <summary>
    /// Gets all registered builder configurations.
    /// </summary>
    public IReadOnlyCollection<object> GetAllBuilders()
    {
        return _builders.Values.ToList().AsReadOnly();
    }
}

/// <summary>
/// Handles durable persistence for half-completed composite context aggregation states.
/// </summary>
public sealed class CompositeContextStore
{
    private readonly IRedisConnectionResolver? _redisResolver;
    private readonly IConfiguration _configuration;
    private readonly KafkaOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="CompositeContextStore"/> class.
    /// </summary>
    public CompositeContextStore(
        IConfiguration configuration,
        IOptions<KafkaOptions> options,
        IServiceProvider serviceProvider)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _redisResolver = serviceProvider.GetService<IRedisConnectionResolver>();
    }

    /// <summary>
    /// Retrieves the accumulated composite context state or instantiates a new one if not found.
    /// </summary>
    public async Task<TComposite> GetAsync<TComposite>(string assemblyKey, CancellationToken cancellationToken)
        where TComposite : class, ICompositeContext, new()
    {
        if (_options.Assembly.Provider == AssemblyProvider.SqlServer)
        {
            var connectionString = _configuration.GetConnectionString(_options.Assembly.ConnectionStringName) ?? _configuration[_options.Assembly.ConnectionStringName];
            var schema = SqlIdentifierValidator.ValidateIdentifier(_options.Assembly.TableSchema, nameof(_options.Assembly.TableSchema));
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            var sql = $"SELECT segment_payload FROM [{schema}].pending_message_assemblies WHERE assembly_key = @AssemblyKey AND segment_index = -1";
            var json = await connection.QueryFirstOrDefaultAsync<string>(sql, new { AssemblyKey = assemblyKey });
            if (string.IsNullOrEmpty(json))
            {
                return new TComposite { AssemblyKey = assemblyKey };
            }

            return JsonSerializer.Deserialize<TComposite>(json, JsonOptions.Default) 
                   ?? new TComposite { AssemblyKey = assemblyKey };
        }
        else
        {
            if (_redisResolver == null) throw new InvalidOperationException("Redis Connection Resolver is not registered.");
            var multiplexer = _redisResolver.Resolve(_options.Assembly.RedisConnectionAlias);
            var db = multiplexer.GetDatabase();
            var redisKey = $"kafka:composite:{{{assemblyKey}}}";
            var json = await db.StringGetAsync(redisKey);
            if (json.IsNullOrEmpty)
            {
                return new TComposite { AssemblyKey = assemblyKey };
            }

            return JsonSerializer.Deserialize<TComposite>((string)json!, JsonOptions.Default)
                   ?? new TComposite { AssemblyKey = assemblyKey };
        }
    }

    /// <summary>
    /// Persists the intermediate composite context state.
    /// </summary>
    public async Task SaveAsync<TComposite>(string assemblyKey, TComposite composite, CancellationToken cancellationToken)
        where TComposite : class, ICompositeContext
    {
        var json = JsonSerializer.Serialize(composite, JsonOptions.Default);
        if (_options.Assembly.Provider == AssemblyProvider.SqlServer)
        {
            var connectionString = _configuration.GetConnectionString(_options.Assembly.ConnectionStringName) ?? _configuration[_options.Assembly.ConnectionStringName];
            var schema = SqlIdentifierValidator.ValidateIdentifier(_options.Assembly.TableSchema, nameof(_options.Assembly.TableSchema));
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            var sql = $@"
                MERGE [{schema}].pending_message_assemblies AS target
                USING (VALUES (@AssemblyKey, -1, @Payload)) 
                    AS source (assembly_key, segment_index, segment_payload)
                ON target.assembly_key = source.assembly_key AND target.segment_index = source.segment_index
                WHEN NOT MATCHED THEN
                    INSERT (assembly_key, segment_index, total_segments, segment_payload)
                    VALUES (source.assembly_key, source.segment_index, 1, source.segment_payload)
                WHEN MATCHED THEN
                    UPDATE SET segment_payload = source.segment_payload;";
            await connection.ExecuteAsync(sql, new { AssemblyKey = assemblyKey, Payload = json });
        }
        else
        {
            if (_redisResolver == null) throw new InvalidOperationException("Redis Connection Resolver is not registered.");
            var multiplexer = _redisResolver.Resolve(_options.Assembly.RedisConnectionAlias);
            var db = multiplexer.GetDatabase();
            var redisKey = $"kafka:composite:{{{assemblyKey}}}";
            var ttl = TimeSpan.FromSeconds(_options.Assembly.StaleThresholdSeconds);
            await db.StringSetAsync(redisKey, json, ttl);
        }
    }

    /// <summary>
    /// Deletes the composite context state upon completion.
    /// </summary>
    public async Task DeleteAsync(string assemblyKey, CancellationToken cancellationToken)
    {
        if (_options.Assembly.Provider == AssemblyProvider.SqlServer)
        {
            var connectionString = _configuration.GetConnectionString(_options.Assembly.ConnectionStringName) ?? _configuration[_options.Assembly.ConnectionStringName];
            var schema = SqlIdentifierValidator.ValidateIdentifier(_options.Assembly.TableSchema, nameof(_options.Assembly.TableSchema));
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            var sql = $"DELETE FROM [{schema}].pending_message_assemblies WHERE assembly_key = @AssemblyKey";
            await connection.ExecuteAsync(sql, new { AssemblyKey = assemblyKey });
        }
        else
        {
            if (_redisResolver == null) throw new InvalidOperationException("Redis Connection Resolver is not registered.");
            var multiplexer = _redisResolver.Resolve(_options.Assembly.RedisConnectionAlias);
            var db = multiplexer.GetDatabase();
            var redisKey = $"kafka:composite:{{{assemblyKey}}}";
            await db.KeyDeleteAsync(redisKey);
        }
    }
}

/// <summary>
/// Handler subscribed to source topics that routes incoming events to the composite context.
/// </summary>
public sealed class CompositeEventSubscriptionHandler<TComposite, TEvent> : IMessageHandler<TEvent>
    where TComposite : class, ICompositeContext, new()
    where TEvent : class
{
    private const int MaxDeferredEventPayloadCharacters = 1048576;
    private readonly CompositeContextStore _store;
    private readonly CompositeMessageRegistry _registry;
    private readonly IServiceProvider _serviceProvider;
    private readonly IConfiguration _configuration;
    private readonly KafkaOptions _options;
    private readonly ILogger<CompositeEventSubscriptionHandler<TComposite, TEvent>> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="CompositeEventSubscriptionHandler{TComposite, TEvent}"/> class.
    /// </summary>
    public CompositeEventSubscriptionHandler(
        CompositeContextStore store,
        CompositeMessageRegistry registry,
        IServiceProvider serviceProvider,
        IConfiguration configuration,
        IOptions<KafkaOptions> options,
        ILogger<CompositeEventSubscriptionHandler<TComposite, TEvent>> logger)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<Result> HandleAsync(TEvent message, CancellationToken cancellationToken)
    {
        if (message is null) throw new ArgumentNullException(nameof(message));

        var builder = _registry.GetBuilder<TComposite>();
        if (builder == null)
        {
            return Result.Failure($"No composite builder registered for type {typeof(TComposite).FullName}");
        }

        var step = builder.Steps.FirstOrDefault(s => s.EventType == typeof(TEvent));
        if (step == null)
        {
            return Result.Failure($"No step configuration registered for event {typeof(TEvent).FullName}");
        }

        var keySelectorFunc = (Func<TEvent, string>)step.KeySelector;
        var assemblyKey = keySelectorFunc(message);

        if (!BoundedIdentifier.TryNormalize(assemblyKey, out assemblyKey))
        {
            return Result.Failure("The composite assembly key is invalid.");
        }

        var composite = await _store.GetAsync<TComposite>(assemblyKey, cancellationToken);

        // Ordered chains validation
        if (builder.IsOrdered && step.PredecessorEventTypeName != null)
        {
            var predecessorProp = typeof(TComposite).GetProperties()
                .FirstOrDefault(p => string.Equals(p.PropertyType.Name, step.PredecessorEventTypeName, StringComparison.Ordinal)
                                     || string.Equals(p.Name, step.PredecessorEventTypeName, StringComparison.OrdinalIgnoreCase));

            var isPredecessorPopulated = predecessorProp != null && predecessorProp.GetValue(composite) != null;
            if (!isPredecessorPopulated)
            {
                // Defer processing using staged jobs table
                _logger.LogInformation(
                    "Event {EventType} received out of sequence because its predecessor is missing. Deferring to staged jobs.",
                    typeof(TEvent).Name);

                return await DeferEventAsync(assemblyKey, message, cancellationToken);
            }
        }

        // Enrich composite context
        var enricherAction = (Action<TComposite, TEvent>)step.Enricher;
        enricherAction(composite, message);

        await _store.SaveAsync(assemblyKey, composite, cancellationToken);

        if (composite.IsReady)
        {
            using var scope = _serviceProvider.CreateScope();
            var handler = (IMessageHandler<TComposite>)scope.ServiceProvider.GetRequiredService(builder.HandlerType!);
            var handlerResult = await handler.HandleAsync(composite, cancellationToken);

            if (!handlerResult.IsSuccess)
            {
                return handlerResult;
            }

            await _store.DeleteAsync(assemblyKey, cancellationToken);
        }

        return Result.Success();
    }

    private async Task<Result> DeferEventAsync(string assemblyKey, TEvent message, CancellationToken cancellationToken)
    {
        try
        {
            var connectionString = _configuration.GetConnectionString(_options.StagedJobs.ConnectionStringName) ?? _configuration[_options.StagedJobs.ConnectionStringName];
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                return Result.Failure("Cannot defer event: staged jobs database connection string is not configured.");
            }

            var schema = SqlIdentifierValidator.ValidateIdentifier(_options.StagedJobs.TableSchema, nameof(_options.StagedJobs.TableSchema));

            var eventPayloadJson = JsonSerializer.Serialize(message, JsonOptions.KafkaCompatible);
            if (eventPayloadJson.Length > MaxDeferredEventPayloadCharacters)
            {
                return Result.Failure("The deferred composite event payload exceeds the supported limit.");
            }

            var jobPayload = new CompositeDeferredEventPayload
            {
                AssemblyKey = assemblyKey,
                EventPayloadJson = eventPayloadJson,
                EventTypeFullName = typeof(TEvent).AssemblyQualifiedName!,
                CompositeTypeFullName = typeof(TComposite).AssemblyQualifiedName!
            };

            var payloadJson = JsonSerializer.Serialize(jobPayload, JsonOptions.KafkaCompatible);

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            var sql = $@"
                INSERT INTO [{schema}].staged_resolve_jobs 
                    (job_type, payload_json, status, max_attempts, next_attempt_at) 
                VALUES 
                    ('CompositeDeferredEvent', @PayloadJson, 'Pending', 5, DATEADD(second, 5, GETUTCDATE()))";

            await connection.ExecuteAsync(new CommandDefinition(sql, new { PayloadJson = payloadJson }, cancellationToken: cancellationToken));

            return Result.Success();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                "Failed to defer composite event to staged jobs. ErrorType: {ErrorType}.",
                exception.GetType().Name);
            return Result.Failure("Deferred event execution enqueue failed.");
        }
    }
}

/// <summary>
/// Serialized payload structure for enqueuing deferred out-of-order aggregator events.
/// </summary>
public sealed class CompositeDeferredEventPayload
{
    /// <summary>
    /// Gets or sets the assembly key identifying the composite aggregation context.
    /// </summary>
    public string AssemblyKey { get; set; } = "";

    /// <summary>
    /// Gets or sets the serialized event message.
    /// </summary>
    public string EventPayloadJson { get; set; } = "";

    /// <summary>
    /// Gets or sets the assembly-qualified name of the event type.
    /// </summary>
    public string EventTypeFullName { get; set; } = "";

    /// <summary>
    /// Gets or sets the assembly-qualified name of the composite context type.
    /// </summary>
    public string CompositeTypeFullName { get; set; } = "";
}

/// <summary>
/// Executor implementing IStagedJobExecutor that retries deferred out-of-order aggregator events.
/// </summary>
public sealed class CompositeDeferredEventExecutor : IStagedJobExecutor
{
    private const int MaxDeferredJobPayloadCharacters = 2097152;
    private const int MaxTypeIdentityCharacters = 1024;
    private readonly CompositeContextStore _store;
    private readonly CompositeMessageRegistry _registry;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<CompositeDeferredEventExecutor> _logger;

    /// <inheritdoc />
    public string JobType => "CompositeDeferredEvent";

    /// <summary>
    /// Initializes a new instance of the <see cref="CompositeDeferredEventExecutor"/> class.
    /// </summary>
    public CompositeDeferredEventExecutor(
        CompositeContextStore store,
        CompositeMessageRegistry registry,
        IServiceProvider serviceProvider,
        ILogger<CompositeDeferredEventExecutor> logger)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<Result> ExecuteAsync(string payloadJson, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(payloadJson)
            || payloadJson.Length > MaxDeferredJobPayloadCharacters)
        {
            return Result.Failure("Deferred job payload is empty.");
        }

        try
        {
            var jobPayload = JsonSerializer.Deserialize<CompositeDeferredEventPayload>(payloadJson, JsonOptions.KafkaCompatible);
            if (jobPayload == null)
            {
                return Result.Failure("Failed to deserialize deferred job payload.");
            }

            if (!BoundedIdentifier.TryNormalize(jobPayload.AssemblyKey, out var normalizedAssemblyKey)
                || jobPayload.EventPayloadJson.Length > MaxDeferredJobPayloadCharacters
                || !IsSafeTypeIdentity(jobPayload.CompositeTypeFullName)
                || !IsSafeTypeIdentity(jobPayload.EventTypeFullName))
            {
                return Result.Failure("The deferred composite job payload is invalid.");
            }

            jobPayload.AssemblyKey = normalizedAssemblyKey;

            var registeredBuilder = _registry.GetAllBuilders().FirstOrDefault(builder =>
                builder.GetType().IsGenericType
                && builder.GetType().GetGenericTypeDefinition() == typeof(CompositeMessageBuilder<>)
                && string.Equals(
                    builder.GetType().GenericTypeArguments[0].AssemblyQualifiedName,
                    jobPayload.CompositeTypeFullName,
                    StringComparison.Ordinal));
            var compositeType = registeredBuilder?.GetType().GenericTypeArguments[0];
            var steps = registeredBuilder?.GetType().GetProperty("Steps")
                ?.GetValue(registeredBuilder) as IEnumerable<CompositeStepDescriptor>;
            var eventType = steps?
                .Select(step => step.EventType)
                .FirstOrDefault(type => string.Equals(
                    type.AssemblyQualifiedName,
                    jobPayload.EventTypeFullName,
                    StringComparison.Ordinal));

            if (compositeType == null || eventType == null)
            {
                return Result.Failure("The deferred composite job references an unregistered type.");
            }

            var method = GetType().GetMethod(nameof(ExecuteGenericAsync), BindingFlags.NonPublic | BindingFlags.Instance)
                ?.MakeGenericMethod(compositeType, eventType);

            if (method == null)
            {
                return Result.Failure("Failed to resolve generic deferred job execution method.");
            }

            var task = (Task<Result>?)method.Invoke(this, new object[] { jobPayload, cancellationToken });
            if (task == null)
            {
                return Result.Failure("Failed to invoke generic deferred job execution method.");
            }

            return await task;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                "Error executing deferred composite event job. ErrorType: {ErrorType}.",
                exception.GetType().Name);
            return Result.Failure("Deferred composite job execution failed.");
        }
    }

    private static bool IsSafeTypeIdentity(string? value)
    {
        return !string.IsNullOrWhiteSpace(value)
            && value.Length <= MaxTypeIdentityCharacters
            && !value.Any(char.IsControl);
    }

    private async Task<Result> ExecuteGenericAsync<TComposite, TEvent>(
        CompositeDeferredEventPayload jobPayload,
        CancellationToken cancellationToken)
        where TComposite : class, ICompositeContext, new()
        where TEvent : class
    {
        var builder = _registry.GetBuilder<TComposite>();
        if (builder == null)
        {
            return Result.Failure($"No composite builder configuration registered for {typeof(TComposite).FullName}.");
        }

        var step = builder.Steps.FirstOrDefault(s => s.EventType == typeof(TEvent));
        if (step == null)
        {
            return Result.Failure($"No step registered for event {typeof(TEvent).FullName} in composite {typeof(TComposite).FullName}.");
        }

        var composite = await _store.GetAsync<TComposite>(jobPayload.AssemblyKey, cancellationToken);

        if (step.PredecessorEventTypeName != null)
        {
            var predecessorProp = typeof(TComposite).GetProperties()
                .FirstOrDefault(p => string.Equals(p.PropertyType.Name, step.PredecessorEventTypeName, StringComparison.Ordinal)
                                     || string.Equals(p.Name, step.PredecessorEventTypeName, StringComparison.OrdinalIgnoreCase));

            var isPredecessorPopulated = predecessorProp != null && predecessorProp.GetValue(composite) != null;
            if (!isPredecessorPopulated)
            {
                return Result.Failure($"Predecessor event '{step.PredecessorEventTypeName}' is still missing in composite context state.");
            }
        }

        var evt = JsonSerializer.Deserialize<TEvent>(jobPayload.EventPayloadJson, JsonOptions.KafkaCompatible);
        if (evt == null)
        {
            return Result.Failure($"Failed to deserialize event payload to type '{typeof(TEvent).FullName}'.");
        }

        // Enrich composite context
        var enricherAction = (Action<TComposite, TEvent>)step.Enricher;
        enricherAction(composite, evt);

        await _store.SaveAsync(jobPayload.AssemblyKey, composite, cancellationToken);

        if (composite.IsReady)
        {
            using var scope = _serviceProvider.CreateScope();
            var handler = (IMessageHandler<TComposite>)scope.ServiceProvider.GetRequiredService(builder.HandlerType!);
            var handlerResult = await handler.HandleAsync(composite, cancellationToken);

            if (!handlerResult.IsSuccess)
            {
                return handlerResult;
            }

            await _store.DeleteAsync(jobPayload.AssemblyKey, cancellationToken);
        }

        return Result.Success();
    }
}
