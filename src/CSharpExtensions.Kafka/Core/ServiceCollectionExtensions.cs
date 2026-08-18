using CSharpExtensions.Core.Security.Interfaces;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core.Pipeline;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Models;
using StackExchange.Redis;
using Swashbuckle.AspNetCore.SwaggerUI;

namespace CSharpExtensions.Kafka.Core;

/// <summary>
/// Service collection extension methods to register and configure the CSharpExtensions.Kafka library.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Kafka Message Bus with all infrastructure services.
    /// Configuration is loaded from the "Kafka" section of <paramref name="configuration"/>.
    /// Use the optional <paramref name="configure"/> delegate to register message subscriptions
    /// and fine-tune options in code.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    /// <param name="configuration">Application configuration containing the "Kafka" section.</param>
    /// <param name="configure">
    /// Optional builder delegate for registering message subscriptions and overriding configuration.
    /// </param>
    public static IServiceCollection AddKafka(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<KafkaBuilder>? configure = null)
    {
        if (services is null) throw new ArgumentNullException(nameof(services));
        if (configuration is null) throw new ArgumentNullException(nameof(configuration));

        // Layer 1: Bind from JSON configuration ("Kafka" section)
        services
            .AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection("Kafka"))
            .ValidateOnStart();
        // Layer 2: Execute builder delegate (subscription registration + option overrides)
        var builder = new KafkaBuilder(services);
        configure?.Invoke(builder);

        // Layer 3: Apply code-based option overrides (PostConfigure runs after JSON binding)
        if (builder.OptionsConfigurator is not null)
        {
            services.PostConfigure(builder.OptionsConfigurator);
        }

        // Layer 4: Startup validation - fail fast on invalid configuration
        services.AddSingleton<IValidateOptions<KafkaOptions>, KafkaOptionsValidator>();

        // Redis connection resolver (multi-instance support)
        // If a single IConnectionMultiplexer is in DI, it auto-resolves as "Default"
        services.AddSingleton<IRedisConnectionResolver>(serviceProvider =>
        {
            var defaultConnection = serviceProvider.GetService<IConnectionMultiplexer>();
            var resolver = new RedisConnectionResolver(defaultConnection);

            // Merge any connections registered via builder.AddRedisConnection()
            foreach (var alias in builder.RedisResolver.GetRegisteredAliases())
            {
                if (!resolver.IsRegistered(alias))
                {
                    resolver.Register(alias, builder.RedisResolver.Resolve(alias));
                }
            }

            return resolver;
        });

        // Caching Producer Manager
        services.AddSingleton<KafkaProducerManager>();

        // Cryptographic message signing & verification
        services.TryAddSingleton<IKafkaSignatureKeyProvider, ConfigurationKafkaSignatureKeyProvider>();
        services.AddSingleton(serviceProvider => new SignatureService(
            serviceProvider.GetService<IEncryptionService>(),
            serviceProvider.GetRequiredService<IOptions<KafkaOptions>>(),
            serviceProvider.GetService<IKafkaSignatureKeyProvider>()));
        services.AddHostedService<KafkaSignatureKeyValidationHostedService>();
        services.AddHostedService<KafkaRuntimeDependencyValidationHostedService>();

        // S3 Offloader for large payload claim checking
        services.AddSingleton<S3ClaimCheckOffloader>();

        // Message schema evolution upcasting registry
        services.AddSingleton<MessageUpcastRegistry>();

        // Redis-backed unique duplicate claims detector
        services.AddSingleton<IDistributedDuplicateDetector, RedisDistributedDuplicateDetector>();

        // Consumer processing pipeline builder (internal)
        services.AddSingleton<ConsumerPipelineBuilder>();

        // Central Message Bus API (publish + internal subscribe)
        services.AddSingleton<KafkaMessageBus>();
        services.AddSingleton<IMessageBus>(serviceProvider => serviceProvider.GetRequiredService<KafkaMessageBus>());

        // Outbox pattern publisher
        services.AddSingleton<IOutboxPublisher, KafkaOutboxPublisher>();

        // Always register the self-disabling worker so JSON and fluent activation have identical behavior.
        services.AddHostedService<KafkaOutboxProcessor>();

        // Topic Administration & Diagnostics (Phase 2)
        services.AddSingleton<IKafkaAdministrationService, KafkaAdministrationService>();
        services.AddSingleton<IKafkaTopicValidator, KafkaTopicValidator>();

        // DB-Staged Repair Pipeline
        services.AddSingleton<IDbStagedRepairPipeline, DbStagedRepairPipeline>();
        services.AddSingleton<KafkaRecoveryManager>();

        // Stateful Message Enrichment (Saga/Aggregator)
        services.AddSingleton(builder.CompositeRegistry);
        services.AddSingleton<CompositeContextStore>();
        services.AddTransient<IStagedJobExecutor, CompositeDeferredEventExecutor>();

        // Message Assembly (Phase 3) - registered only when UseMessageAssembly is called
        if (builder.MessageAssemblyEnabled)
        {
            services.AddSingleton<IMessageAssembler>(serviceProvider =>
            {
                var kafkaOptions = serviceProvider.GetRequiredService<IOptions<KafkaOptions>>().Value;
                if (kafkaOptions.Assembly.Provider == AssemblyProvider.SqlServer)
                {
                    return ActivatorUtilities.CreateInstance<SqlServerMessageAssembler>(serviceProvider);
                }

                return ActivatorUtilities.CreateInstance<RedisMessageAssembler>(serviceProvider);
            });
        }

        // Staged Job Engine (Phase 4) - registered only when UseStagedJobs is called
        if (builder.StagedJobsEnabled)
        {
            services.AddHostedService<StagedJobProcessor>();
        }

        // Maintenance Background Service (Phase 4) - registered only when UseMaintenance is called
        if (builder.MaintenanceEnabled)
        {
            services.AddHostedService<KafkaMaintenanceHostedService>();
        }

        // Maintenance HTTP Endpoints (Phase 5) - registered only when UseMaintenanceEndpoints is called
        if (builder.MaintenanceEndpointsEnabled)
        {
            services.AddSingleton<IKafkaMaintenanceService, KafkaMaintenanceService>();
            services.AddAuthorization(options =>
            {
                options.AddPolicy(KafkaMaintenancePolicies.Read, policy =>
                    policy.RequireAuthenticatedUser().RequireClaim(
                        KafkaMaintenancePolicies.PermissionClaim,
                        KafkaMaintenancePolicies.ReadPermission,
                        KafkaMaintenancePolicies.WritePermission));
                options.AddPolicy(KafkaMaintenancePolicies.Write, policy =>
                    policy.RequireAuthenticatedUser().RequireClaim(
                        KafkaMaintenancePolicies.PermissionClaim,
                        KafkaMaintenancePolicies.WritePermission));
            });

            // Register the controller assembly part without calling AddControllers(),
            // which would override or interfere with the host application's MVC configuration
            services.ConfigureOptions<KafkaMaintenanceApplicationPartConfigurator>();

            // Register Swagger document and UI endpoint for Kafka Maintenance group natively
            services.ConfigureSwaggerGen(options =>
            {
                options.SwaggerDoc("Kafka Maintenance", new OpenApiInfo
                {
                    Title = "Kafka Maintenance",
                    Version = "v1",
                    Description = "Kafka infrastructure maintenance and diagnostics endpoints."
                });

                var xmlFile = $"{typeof(KafkaMaintenanceController).Assembly.GetName().Name}.xml";
                var xmlPath = System.IO.Path.Combine(AppContext.BaseDirectory, xmlFile);
                if (System.IO.File.Exists(xmlPath))
                {
                    options.IncludeXmlComments(xmlPath);
                }

                options.ParameterFilter<KafkaMaintenanceParameterFilter>();
            });

            services.Configure<SwaggerUIOptions>(options =>
            {
                options.SwaggerEndpoint("Kafka%20Maintenance/swagger.json", "Kafka Maintenance");
            });
        }

        // Auto-start message subscriptions registered via builder.Subscribe<TMessage>()
        services.AddSingleton<IReadOnlyList<MessageSubscriptionDescriptor>>(builder.Subscriptions.AsReadOnly());
        if (builder.Subscriptions.Count > 0)
        {
            services.AddHostedService<KafkaSubscriptionHostedService>();
        }

        return services;
    }
}
