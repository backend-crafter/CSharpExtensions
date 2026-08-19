using CSharpExtensions.Kafka.Validation;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Generic;
using System.Reflection;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Fluent builder for configuring Kafka message subscriptions and library options.
/// Used inside the <c>AddKafka</c> registration delegate.
/// </summary>
/// <example>
/// <code>
/// services.AddKafka(configuration, kafka =>
/// {
///     kafka.Configure(options =>
///     {
///         options.Producer.CompressionType = "Snappy";
///         options.Consumer.SessionTimeoutMs = 45000;
///     });
///
///     kafka.Subscribe&lt;EligibleWagerFactRecorded&gt;(options =>
///     {
///         options.AddHandler&lt;EligibleWagerFactRecordedHandler&gt;();
///         options.AddUpcastChainResolver(UpcasterGenerationMode.OnlyOnce);
///     });
/// });
/// </code>
/// </example>
public sealed class KafkaBuilder
{
    private readonly IServiceCollection _services;
    internal List<MessageSubscriptionDescriptor> Subscriptions { get; } = new();
    internal Action<KafkaOptions>? OptionsConfigurator { get; private set; }
    internal bool OutboxEnabled { get; private set; }
    internal bool MessageAssemblyEnabled { get; private set; }
    internal bool StagedJobsEnabled { get; private set; }
    internal bool MaintenanceEnabled { get; private set; }
    internal bool MaintenanceEndpointsEnabled { get; private set; }
    internal RedisConnectionResolver RedisResolver { get; } = new();
    internal CompositeMessageRegistry CompositeRegistry { get; } = new();

    internal KafkaBuilder(IServiceCollection services)
    {
        _services = services ?? throw new ArgumentNullException(nameof(services));
    }

    /// <summary>
    /// Registers a named Redis connection for multi-instance support.
    /// Different Kafka features (deduplication, message assembly, distributed locks)
    /// can use separate Redis instances by specifying different aliases.
    /// </summary>
    /// <param name="connectionAlias">A unique alias for this connection (e.g., "Dedup", "Assembly").</param>
    /// <param name="connectionMultiplexer">The Redis connection multiplexer instance.</param>
    /// <example>
    /// <code>
    /// services.AddKafka(configuration, kafka =>
    /// {
    ///     kafka.AddRedisConnection("Dedup", dedupRedis);
    ///     kafka.AddRedisConnection("Assembly", assemblyRedis);
    /// });
    /// </code>
    /// </example>
    public KafkaBuilder AddRedisConnection(string connectionAlias, StackExchange.Redis.IConnectionMultiplexer connectionMultiplexer)
    {
        RedisResolver.Register(connectionAlias, connectionMultiplexer);
        return this;
    }

    /// <summary>
    /// Applies code-based overrides to <see cref="KafkaOptions"/> after JSON configuration binding.
    /// Use this for advanced tuning that doesn't need to vary per environment.
    /// </summary>
    public KafkaBuilder Configure(Action<KafkaOptions> configure)
    {
        if (configure is null) throw new ArgumentNullException(nameof(configure));
        OptionsConfigurator += configure;
        return this;
    }

    /// <summary>
    /// Enables the transactional outbox pattern for guaranteed event delivery.
    /// Auto-provisions the <c>dbo.kafka_outbox</c> table on startup and starts the background processor.
    /// </summary>
    /// <param name="connectionStringName">
    /// The key from the <c>ConnectionStrings</c> configuration section pointing to the service database
    /// where the outbox table will be created (e.g., <c>"VipConnectionString"</c>, <c>"DefaultConnection"</c>).
    /// </param>
    /// <param name="tableSchema">The database schema where the outbox table will be created and processed (defaults to "dbo").</param>
    /// <param name="configure">Optional delegate to override batch size, polling interval, etc.</param>
    /// <example>
    /// <code>
    /// services.AddKafka(configuration, kafka =>
    /// {
    ///     kafka.UseOutbox("VipConnectionString");
    /// });
    /// </code>
    /// </example>
    public KafkaBuilder UseOutbox(string connectionStringName, string tableSchema = "dbo", Action<KafkaOutboxSettings>? configure = null)
    {
        if (string.IsNullOrWhiteSpace(connectionStringName))
            throw new ArgumentException("Connection string name must not be empty.", nameof(connectionStringName));
        if (string.IsNullOrWhiteSpace(tableSchema))
            throw new ArgumentException("Table schema must not be empty.", nameof(tableSchema));

        OptionsConfigurator += options =>
        {
            options.Outbox.IsEnabled = true;
            options.Outbox.ConnectionStringName = connectionStringName;
            options.Outbox.TableSchema = tableSchema;
            configure?.Invoke(options.Outbox);
        };

        OutboxEnabled = true;
        return this;
    }

    /// <summary>
    /// Registers a message subscription. The subscription is automatically started when the application starts.
    /// Use <c>AddHandler</c> in the configure delegate for automatic processing,
    /// or omit it and inject <see cref="IKafkaConsumer{TMessage}"/> for manual consumption.
    /// </summary>
    /// <typeparam name="TMessage">
    /// The message type. Must match a key in <c>Kafka:Topics</c> configuration
    /// (matched by class name or [Topic] attribute).
    /// </typeparam>
    /// <param name="configure">Optional delegate to configure handler, consumer group, read mode, and upcasting.</param>
    /// <example>
    /// <code>
    /// // With handler (automatic processing):
    /// kafka.Subscribe&lt;EligibleWagerFactRecorded&gt;(options =>
    /// {
    ///     options.AddHandler&lt;EligibleWagerFactRecordedHandler&gt;();
    /// });
    ///
    /// // Without handler (manual consumption via IKafkaConsumer):
    /// kafka.Subscribe&lt;EligibleWagerFactRecorded&gt;();
    /// </code>
    /// </example>
    public KafkaBuilder Subscribe<TMessage>(Action<KafkaSubscriptionBuilder<TMessage>>? configure = null)
        where TMessage : class
    {
        // Enforce naming conventions at registration time
        MessageNamingConventionValidator.Validate<TMessage>();

        var builder = new KafkaSubscriptionBuilder<TMessage>();
        configure?.Invoke(builder);

        var subscriptionOptions = new KafkaSubscriptionOptions
        {
            ReadMode = builder.ReadMode,
            ConsumerGroup = builder.ConsumerGroup,
            StartOffsetTime = builder.StartOffsetTime
        };

        if (builder.HandlerType is not null)
        {
            // Handler mode: register handler as Scoped (new instance per message scope)
            _services.AddScoped(builder.HandlerType);

            Subscriptions.Add(new MessageSubscriptionDescriptor(
                messageType: typeof(TMessage),
                handlerType: builder.HandlerType,
                options: subscriptionOptions,
                mode: SubscriptionMode.Handler));
        }
        else
        {
            // Consumer mode: register IKafkaConsumer<TMessage> for manual consumption
            _services.AddSingleton<KafkaConsumer<TMessage>>();
            _services.AddSingleton<IKafkaConsumer<TMessage>>(serviceProvider =>
                serviceProvider.GetRequiredService<KafkaConsumer<TMessage>>());

            Subscriptions.Add(new MessageSubscriptionDescriptor(
                messageType: typeof(TMessage),
                handlerType: null,
                options: subscriptionOptions,
                mode: SubscriptionMode.Consumer));
        }

        return this;
    }

    /// <summary>
    /// Registers a producer message type for convention validation.
    /// </summary>
    public KafkaBuilder RegisterProducer<TMessage>() where TMessage : class
    {
        MessageNamingConventionValidator.Validate<TMessage>();
        return this;
    }

    /// <summary>
    /// Registers a stateful message enrichment composite context and its event aggregation rules.
    /// </summary>
    /// <typeparam name="TComposite">The type of the composite context.</typeparam>
    /// <param name="configure">The configuration delegate to define aggregation steps and handler.</param>
    /// <returns>The builder instance for chaining.</returns>
    public KafkaBuilder RegisterComposite<TComposite>(Action<CompositeMessageBuilder<TComposite>> configure)
        where TComposite : class, ICompositeContext, new()
    {
        if (configure is null) throw new ArgumentNullException(nameof(configure));

        var builder = new CompositeMessageBuilder<TComposite>();
        configure(builder);

        // Register the composite context handler as scoped if one is configured
        if (builder.HandlerType is not null)
        {
            _services.AddScoped(builder.HandlerType);
        }

        // Register the composite aggregation builder in the registry
        CompositeRegistry.Register(builder);

        // Dynamically register subscription for each event type in the composite steps
        var registerStepMethod = typeof(KafkaBuilder)
            .GetMethod(nameof(RegisterCompositeStepSubscription), BindingFlags.NonPublic | BindingFlags.Instance);

        if (registerStepMethod == null)
        {
            throw new InvalidOperationException("Could not find RegisterCompositeStepSubscription method.");
        }

        foreach (var step in builder.Steps)
        {
            var genericMethod = registerStepMethod.MakeGenericMethod(typeof(TComposite), step.EventType);
            genericMethod.Invoke(this, null);
        }

        return this;
    }

    private void RegisterCompositeStepSubscription<TComposite, TEvent>()
        where TComposite : class, ICompositeContext, new()
        where TEvent : class
    {
        MessageNamingConventionValidator.Validate<TEvent>();

        // Register the scoped event subscription handler
        _services.AddScoped<CompositeEventSubscriptionHandler<TComposite, TEvent>>();

        var subscriptionOptions = new KafkaSubscriptionOptions
        {
            ReadMode = KafkaReadMode.Latest
        };

        Subscriptions.Add(new MessageSubscriptionDescriptor(
            messageType: typeof(TEvent),
            handlerType: typeof(CompositeEventSubscriptionHandler<TComposite, TEvent>),
            options: subscriptionOptions,
            mode: SubscriptionMode.Handler));
    }

    /// <summary>
    /// Configures global options for Redis-backed idempotency (deduplication).
    /// </summary>
    /// <param name="configure">Optional delegate to override global idempotency settings.</param>
    /// <example>
    /// <code>
    /// services.AddKafka(configuration, kafka =>
    /// {
    ///     kafka.UseIdempotency(options =>
    ///     {
    ///         options.RedisConnectionAlias = "Dedup";
    ///     });
    /// });
    /// </code>
    /// </example>
    public KafkaBuilder UseIdempotency(Action<IdempotencyOptions>? configure = null)
    {
        OptionsConfigurator += options =>
        {
            configure?.Invoke(options.Idempotency);
        };
        return this;
    }

    /// <summary>
    /// Enables the multi-segment message assembly feature.
    /// Segments are reassembled using Redis (default) or SQL Server before processing.
    /// </summary>
    /// <param name="configure">Optional delegate to override assembly settings.</param>
    /// <example>
    /// <code>
    /// services.AddKafka(configuration, kafka =>
    /// {
    ///     kafka.UseMessageAssembly(options =>
    ///     {
    ///         options.Provider = AssemblyProvider.Redis;
    ///         options.RedisConnectionAlias = "Assembly";
    ///     });
    /// });
    /// </code>
    /// </example>
    public KafkaBuilder UseMessageAssembly(Action<MessageAssemblyOptions>? configure = null)
    {
        OptionsConfigurator += options =>
        {
            options.Assembly.IsEnabled = true;
            configure?.Invoke(options.Assembly);
        };

        MessageAssemblyEnabled = true;
        return this;
    }

    /// <summary>
    /// Enables the staged resolve job engine for delayed event processing with retry logic.
    /// </summary>
    /// <param name="connectionStringName">
    /// The key from the <c>ConnectionStrings</c> configuration section pointing to the jobs database.
    /// </param>
    /// <param name="tableSchema">The database schema for the staged_resolve_jobs table (defaults to "dbo").</param>
    /// <param name="configure">Optional delegate to override job processing settings.</param>
    /// <example>
    /// <code>
    /// services.AddKafka(configuration, kafka =>
    /// {
    ///     kafka.UseStagedJobs("DefaultConnection");
    /// });
    /// </code>
    /// </example>
    public KafkaBuilder UseStagedJobs(string connectionStringName, string tableSchema = "dbo", Action<StagedJobSettings>? configure = null)
    {
        if (string.IsNullOrWhiteSpace(connectionStringName))
            throw new ArgumentException("Connection string name must not be empty.", nameof(connectionStringName));
        if (string.IsNullOrWhiteSpace(tableSchema))
            throw new ArgumentException("Table schema must not be empty.", nameof(tableSchema));

        OptionsConfigurator += options =>
        {
            options.StagedJobs.IsEnabled = true;
            options.StagedJobs.ConnectionStringName = connectionStringName;
            options.StagedJobs.TableSchema = tableSchema;
            configure?.Invoke(options.StagedJobs);
        };

        StagedJobsEnabled = true;
        return this;
    }

    /// <summary>
    /// Enables the automated maintenance background service.
    /// Handles cleanup of stale assemblies, completed jobs, and permanently failed outbox records.
    /// </summary>
    /// <param name="configure">Optional delegate to override maintenance settings.</param>
    /// <example>
    /// <code>
    /// services.AddKafka(configuration, kafka =>
    /// {
    ///     kafka.UseMaintenance(options =>
    ///     {
    ///         options.IntervalMinutes = 30;
    ///     });
    /// });
    /// </code>
    /// </example>
    public KafkaBuilder UseMaintenance(Action<KafkaMaintenanceSettings>? configure = null)
    {
        OptionsConfigurator += options =>
        {
            configure?.Invoke(options.Maintenance);
        };

        MaintenanceEnabled = true;
        return this;
    }

    /// <summary>
    /// Registers the <see cref="KafkaMaintenanceController"/> providing HTTP endpoints
    /// for Kafka infrastructure maintenance and diagnostics.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The controller is registered under <c>/api/v1/kafka-maintenance</c> and provides
    /// endpoints for DLQ replay, topic validation, assembly cleanup, staged job retry,
    /// and outbox health monitoring.
    /// </para>
    /// <para>
    /// All endpoints appear in OpenAPI/Scalar under the <c>Kafka Maintenance</c> group
    /// and are protected with <c>[Authorize]</c>. A valid authentication scheme
    /// must be configured in the consuming application.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// services.AddKafka(configuration, kafka =>
    /// {
    ///     kafka.UseMaintenanceEndpoints();
    /// });
    /// </code>
    /// </example>
    public KafkaBuilder UseMaintenanceEndpoints()
    {
        MaintenanceEndpointsEnabled = true;
        return this;
    }

    /// <summary>
    /// Registers a topic repair configuration for the specified target message type.
    /// </summary>
    /// <typeparam name="T">The target message type to repair.</typeparam>
    /// <param name="connectionStringName">The name of the connection string in configuration.</param>
    /// <param name="tableSchema">The database schema where the staging table is located (defaults to "dbo").</param>
    /// <param name="configure">Optional configuration delegate for advanced settings.</param>
    /// <returns>The builder instance for chaining.</returns>
    public KafkaBuilder UseTopicRepair<T>(
        string connectionStringName,
        string tableSchema = "dbo",
        Action<KafkaRepairSettings>? configure = null)
        where T : class
    {
        if (string.IsNullOrWhiteSpace(connectionStringName))
            throw new ArgumentException("Connection string name must not be empty.", nameof(connectionStringName));
        if (string.IsNullOrWhiteSpace(tableSchema))
            throw new ArgumentException("Table schema must not be empty.", nameof(tableSchema));

        var settings = new KafkaRepairSettings
        {
            ConnectionStringName = connectionStringName,
            TableSchema = tableSchema
        };
        configure?.Invoke(settings);

        SqlIdentifierValidator.ValidateIdentifier(settings.TableSchema, nameof(settings.TableSchema));
        if (settings.ExportBatchSize is < 1 or > 10000
            || settings.UpcastBatchSize is < 1 or > 10000
            || settings.RepopulateBatchSize is < 1 or > 10000)
        {
            throw new ArgumentOutOfRangeException(
                nameof(configure),
                "Kafka repair batch sizes must be between 1 and 10000.");
        }

        if (settings.DistributedLockTimeoutMs is < 1000 or > 300000)
        {
            throw new ArgumentOutOfRangeException(
                nameof(configure),
                "Kafka repair distributed lock timeout must be between 1000 and 300000 milliseconds.");
        }

        var configurationKey = TopicAttributeResolver.Resolve<T>();

        var repairConfiguration = new KafkaRepairConfiguration(typeof(T), configurationKey, settings);

        _services.AddSingleton(repairConfiguration);

        return this;
    }
}
