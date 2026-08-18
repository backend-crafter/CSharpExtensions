using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using TopicMetadata = CSharpExtensions.Kafka.Abstractions.TopicMetadata;

/// <summary>
/// Provides topic provisioning and metadata operations using the Confluent AdminClient.
/// Resolves cluster connection details from <see cref="KafkaOptions"/> configuration.
/// </summary>
internal sealed class KafkaAdministrationService : IKafkaAdministrationService
{
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaAdministrationService> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaAdministrationService"/> class.
    /// </summary>
    /// <param name="options">Kafka configuration options.</param>
    /// <param name="logger">Logger instance.</param>
    public KafkaAdministrationService(
        IOptions<KafkaOptions> options,
        ILogger<KafkaAdministrationService> logger)
    {
        if (options is null) throw new ArgumentNullException(nameof(options));
        _options = options.Value;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<Result> CreateTopicAsync(
        string topicName,
        int partitionCount,
        short replicationFactor,
        Dictionary<string, string>? topicConfigs = null,
        string? clusterAlias = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            return Result.Failure("Topic name cannot be empty.");

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            using var adminClient = BuildAdminClient(clusterAlias);

            var topicSpecification = new TopicSpecification
            {
                Name = topicName,
                NumPartitions = partitionCount,
                ReplicationFactor = replicationFactor,
                Configs = topicConfigs
            };

            await adminClient.CreateTopicsAsync(
                new[] { topicSpecification },
                new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) });

            _logger.LogInformation(
                "Successfully created topic '{TopicName}' with {PartitionCount} partitions and replication factor {ReplicationFactor}.",
                topicName, partitionCount, replicationFactor);

            return Result.Success();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (CreateTopicsException exception)
        {
            var error = exception.Results.FirstOrDefault()?.Error;
            if (error?.Code == ErrorCode.TopicAlreadyExists)
            {
                _logger.LogInformation("Topic '{TopicName}' already exists.", topicName);
                return Result.Success();
            }

            _logger.LogError(
                "Failed to create Kafka topic. ErrorCode: {ErrorCode}.",
                error?.Code.ToString() ?? "Unknown");
            return Result.Failure("Kafka topic creation failed.");
        }
        catch (KafkaException exception)
        {
            _logger.LogError("Kafka admin topic creation failed. ErrorCode: {ErrorCode}.", exception.Error.Code);
            return Result.Failure("Kafka topic creation failed.");
        }
        catch (Exception exception)
        {
            _logger.LogError("Unexpected Kafka topic creation failure. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure("Kafka topic creation failed.");
        }
    }

    /// <inheritdoc />
    public async Task<Result> DeleteTopicAsync(
        string topicName,
        string? clusterAlias = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            return Result.Failure("Topic name cannot be empty.");

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            using var adminClient = BuildAdminClient(clusterAlias);

            await adminClient.DeleteTopicsAsync(
                new[] { topicName },
                new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) });

            _logger.LogInformation("Successfully deleted topic '{TopicName}'.", topicName);

            return Result.Success();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (DeleteTopicsException exception)
        {
            var error = exception.Results.FirstOrDefault()?.Error;
            _logger.LogError(
                "Failed to delete Kafka topic. ErrorCode: {ErrorCode}.",
                error?.Code.ToString() ?? "Unknown");
            return Result.Failure("Kafka topic deletion failed.");
        }
        catch (KafkaException exception)
        {
            _logger.LogError("Kafka admin topic deletion failed. ErrorCode: {ErrorCode}.", exception.Error.Code);
            return Result.Failure("Kafka topic deletion failed.");
        }
        catch (Exception exception)
        {
            _logger.LogError("Unexpected Kafka topic deletion failure. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure("Kafka topic deletion failed.");
        }
    }

    /// <inheritdoc />
    public async Task<Result<TopicMetadata>> GetTopicMetadataAsync(
        string topicName,
        string? clusterAlias = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            return Result.Failure<TopicMetadata>("Topic name cannot be empty.");

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            using var adminClient = BuildAdminClient(clusterAlias);

            // Retrieve partition and replication metadata
            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(15));
            var brokerTopicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);

            if (brokerTopicMetadata is null)
            {
                return Result.Failure<TopicMetadata>("Kafka topic was not found on the cluster.");
            }

            if (brokerTopicMetadata.Error is not null && brokerTopicMetadata.Error.Code != ErrorCode.NoError)
            {
                return Result.Failure<TopicMetadata>(
                    "Kafka broker rejected the topic metadata request.");
            }

            var partitionCount = brokerTopicMetadata.Partitions.Count;
            var replicationFactor = brokerTopicMetadata.Partitions.FirstOrDefault()?.Replicas.Length ?? 0;

            // Retrieve topic-level configuration
            var configResource = new ConfigResource { Name = topicName, Type = ResourceType.Topic };
            var describeResults = await adminClient.DescribeConfigsAsync(
                new[] { configResource },
                new DescribeConfigsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });

            var configuration = new Dictionary<string, string>();
            var topicConfigResult = describeResults.FirstOrDefault();
            if (topicConfigResult?.Entries is not null)
            {
                foreach (var entry in topicConfigResult.Entries)
                {
                    configuration[entry.Key] = entry.Value.Value ?? string.Empty;
                }
            }

            var result = new TopicMetadata(
                topicName,
                partitionCount,
                replicationFactor,
                configuration);

            _logger.LogInformation(
                "Retrieved metadata for topic '{TopicName}': {PartitionCount} partitions, replication factor {ReplicationFactor}.",
                topicName, partitionCount, replicationFactor);

            return Result.Success(result);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (KafkaException exception)
        {
            _logger.LogError("Kafka admin metadata request failed. ErrorCode: {ErrorCode}.", exception.Error.Code);
            return Result.Failure<TopicMetadata>("Kafka topic metadata request failed.");
        }
        catch (Exception exception)
        {
            _logger.LogError("Unexpected Kafka metadata request failure. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure<TopicMetadata>("Kafka topic metadata request failed.");
        }
    }

    /// <summary>
    /// Builds an <see cref="IAdminClient"/> for the resolved cluster alias.
    /// Falls back to <see cref="KafkaOptions.Servers"/> if the alias is not found in Clusters.
    /// </summary>
    /// <param name="clusterAlias">Optional cluster alias override.</param>
    /// <returns>A configured admin client instance.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when no cluster configuration or fallback servers are available.
    /// </exception>
    private IAdminClient BuildAdminClient(string? clusterAlias)
    {
        var resolvedAlias = string.IsNullOrWhiteSpace(clusterAlias)
            ? _options.DefaultClusterAlias
            : clusterAlias;

        if (!_options.Clusters.TryGetValue(resolvedAlias, out var clusterConfig))
        {
            if (!string.IsNullOrWhiteSpace(_options.Servers))
            {
                clusterConfig = new KafkaClusterConfiguration { BootstrapServers = _options.Servers };
            }
            else
            {
                throw new InvalidOperationException(
                    $"Kafka cluster configuration for '{resolvedAlias}' is not defined and no root 'Servers' fallback is configured.");
            }
        }

        var config = new AdminClientConfig
        {
            BootstrapServers = clusterConfig.BootstrapServers
        };

        if (!string.IsNullOrWhiteSpace(clusterConfig.SecurityProtocol) &&
            Enum.TryParse<SecurityProtocol>(clusterConfig.SecurityProtocol, true, out var protocol))
        {
            config.SecurityProtocol = protocol;
        }

        if (!string.IsNullOrWhiteSpace(clusterConfig.SaslMechanism) &&
            Enum.TryParse<SaslMechanism>(clusterConfig.SaslMechanism, true, out var mechanism))
        {
            config.SaslMechanism = mechanism;
        }

        if (!string.IsNullOrWhiteSpace(clusterConfig.SaslUsername) &&
            !string.IsNullOrWhiteSpace(clusterConfig.SaslPassword))
        {
            config.SaslUsername = clusterConfig.SaslUsername;
            config.SaslPassword = clusterConfig.SaslPassword;
        }

        return new AdminClientBuilder(config).Build();
    }
}
