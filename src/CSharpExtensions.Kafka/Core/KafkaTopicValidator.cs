using CSharpExtensions.Foundation.Helpers.Constants;
using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Scans a Kafka topic and validates message structural integrity by checking
/// required headers, JSON payload validity, and optional signature authenticity.
/// Uses a temporary consumer with a random group ID that is disposed after scanning.
/// </summary>
internal sealed class KafkaTopicValidator : IKafkaTopicValidator
{
    /// <summary>
    /// Maximum number of unique error patterns to retain to prevent OOM under high volume.
    /// </summary>
    private const int MaxUniqueErrorPatterns = 100;

    private readonly KafkaOptions _options;
    private readonly SignatureService _signatureService;
    private readonly ILogger<KafkaTopicValidator> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaTopicValidator"/> class.
    /// </summary>
    /// <param name="options">Kafka configuration options.</param>
    /// <param name="signatureService">Message signature verification service.</param>
    /// <param name="logger">Logger instance.</param>
    public KafkaTopicValidator(
        IOptions<KafkaOptions> options,
        SignatureService signatureService,
        ILogger<KafkaTopicValidator> logger)
    {
        if (options is null) throw new ArgumentNullException(nameof(options));
        _options = options.Value;
        _signatureService = signatureService ?? throw new ArgumentNullException(nameof(signatureService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<Result<TopicValidationReport>> ValidateAsync(
        string topicConfigurationKey,
        int maxMessages = 1000,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicConfigurationKey))
            return Result.Failure<TopicValidationReport>("Topic configuration key cannot be empty.");

        if (maxMessages <= 0)
            return Result.Failure<TopicValidationReport>("Maximum messages to scan must be greater than zero.");

        cancellationToken.ThrowIfCancellationRequested();

        if (!_options.Topics.TryGetValue(topicConfigurationKey, out var topicConfig))
        {
            return Result.Failure<TopicValidationReport>("Kafka topic configuration is not defined.");
        }

        var topicName = topicConfig.TopicName;
        if (string.IsNullOrWhiteSpace(topicName))
        {
            return Result.Failure<TopicValidationReport>("Kafka physical topic name is not configured.");
        }

        try
        {
            var report = await Task.Run(
                () => ScanTopic(topicConfig, maxMessages, cancellationToken),
                cancellationToken);

            return Result.Success(report);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                "Unexpected error while validating Kafka topic. ErrorType: {ErrorType}.",
                exception.GetType().Name);
            return Result.Failure<TopicValidationReport>("Kafka topic validation failed.");
        }
    }

    /// <summary>
    /// Creates a temporary consumer, assigns all partitions from the beginning offset,
    /// and scans up to <paramref name="maxMessages"/> messages for structural validity.
    /// </summary>
    /// <param name="topicConfig">The topic configuration to scan.</param>
    /// <param name="maxMessages">Maximum number of messages to consume.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The validation report summarizing scan results.</returns>
    private TopicValidationReport ScanTopic(
        KafkaTopicConfiguration topicConfig,
        int maxMessages,
        CancellationToken cancellationToken)
    {
        var topicName = topicConfig.TopicName;
        var errors = new List<ValidationError>();
        var totalMessagesScanned = 0;
        var validMessages = 0;
        var invalidMessages = 0;

        var clusterAlias = string.IsNullOrWhiteSpace(topicConfig.Cluster)
            ? _options.DefaultClusterAlias
            : topicConfig.Cluster;

        var bootstrapServers = ResolveBootstrapServers(clusterAlias);

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            AllowAutoCreateTopics = false,
            GroupId = $"kafka-validator-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnablePartitionEof = true
        };

        ApplyClusterSecurity(consumerConfig, clusterAlias, topicConfig);

        // Discover partitions via AdminClient (IConsumer does not expose GetMetadata)
        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
        ApplyClusterSecurityToAdmin(adminConfig, clusterAlias);

        using var adminClient = new AdminClientBuilder(adminConfig).Build();
        var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(15));
        var brokerTopicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);

        if (brokerTopicMetadata is null || brokerTopicMetadata.Partitions.Count == 0)
        {
            _logger.LogWarning("Topic '{TopicName}' has no partitions or was not found.", topicName);
            return new TopicValidationReport(topicName, 0, 0, 0, errors);
        }

        var topicPartitions = brokerTopicMetadata.Partitions
            .Select(p => new TopicPartitionOffset(topicName, new Partition(p.PartitionId), Offset.Beginning))
            .ToList();

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, error) =>
                _logger.LogWarning(
                    "Validator consumer error on topic '{TopicName}'. ErrorCode: {ErrorCode}; IsFatal: {IsFatal}.",
                    topicName,
                    error.Code,
                    error.IsFatal))
            .Build();

        consumer.Assign(topicPartitions);

        var eofPartitions = new HashSet<int>();
        var totalPartitions = brokerTopicMetadata.Partitions.Count;

        _logger.LogInformation(
            "Starting validation scan on topic '{TopicName}' ({PartitionCount} partitions, max {MaxMessages} messages).",
            topicName, totalPartitions, maxMessages);

        while (totalMessagesScanned < maxMessages && !cancellationToken.IsCancellationRequested)
        {
            var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));

            if (consumeResult is null)
            {
                // All partitions reached EOF or timeout
                if (eofPartitions.Count >= totalPartitions)
                    break;

                continue;
            }

            if (consumeResult.IsPartitionEOF)
            {
                eofPartitions.Add(consumeResult.Partition.Value);
                if (eofPartitions.Count >= totalPartitions)
                    break;

                continue;
            }

            totalMessagesScanned++;

            var messageErrors = ValidateMessage(
                consumeResult,
                topicConfig.EnableAuthentication,
                topicConfig);

            if (messageErrors.Count == 0)
            {
                validMessages++;
            }
            else
            {
                invalidMessages++;

                if (errors.Count < MaxUniqueErrorPatterns)
                {
                    errors.AddRange(messageErrors);

                    // Trim to max unique error patterns
                    if (errors.Count > MaxUniqueErrorPatterns)
                    {
                        errors.RemoveRange(MaxUniqueErrorPatterns, errors.Count - MaxUniqueErrorPatterns);
                    }
                }
            }
        }

        consumer.Close();

        _logger.LogInformation(
            "Validation scan completed on topic '{TopicName}': {TotalScanned} scanned, {Valid} valid, {Invalid} invalid.",
            topicName, totalMessagesScanned, validMessages, invalidMessages);

        return new TopicValidationReport(topicName, totalMessagesScanned, validMessages, invalidMessages, errors);
    }

    /// <summary>
    /// Validates a single consumed message for required headers and JSON payload validity.
    /// </summary>
    /// <param name="consumeResult">The consumed Kafka message.</param>
    /// <param name="authenticationEnabled">Whether authentication signature is required.</param>
    /// <param name="topicConfig">Optional topic context required for canonical HMAC v2 verification.</param>
    /// <returns>A list of validation errors found in this message. Empty if valid.</returns>
    internal List<ValidationError> ValidateMessage(
        ConsumeResult<string, string> consumeResult,
        bool authenticationEnabled,
        KafkaTopicConfiguration? topicConfig = null)
    {
        var errors = new List<ValidationError>();
        var offset = consumeResult.Offset.Value;
        var partition = consumeResult.Partition.Value;

        IReadOnlyDictionary<string, string> headers;
        try
        {
            headers = ConsumedMessageHeaders.Extract<object>(
                consumeResult,
                collectRawHeaders: true,
                allowGeneratedMessageIdFallback: false).RawHeaders;
        }
        catch (Exception exception) when (exception is InvalidDataException or OverflowException)
        {
            errors.Add(new ValidationError(offset, partition, "InvalidHeaders",
                "Message transport headers violate the runtime safety policy."));
            return errors;
        }

        // Check required headers
        var hasMessageId = headers.TryGetValue(CustomRequestHeaders.MessageId, out var messageId)
            && !string.IsNullOrWhiteSpace(messageId);
        if (!hasMessageId)
        {
            errors.Add(new ValidationError(offset, partition, "MissingHeader",
                $"Required header '{CustomRequestHeaders.MessageId}' is missing."));
        }

        var hasCorrelationId = headers.TryGetValue(CustomRequestHeaders.CorrelationId, out var correlationId)
            && !string.IsNullOrWhiteSpace(correlationId);
        if (!hasCorrelationId)
        {
            errors.Add(new ValidationError(offset, partition, "MissingHeader",
                $"Required header '{CustomRequestHeaders.CorrelationId}' is missing."));
        }

        if (!headers.TryGetValue(CustomRequestHeaders.EventSchemaVersion, out var schemaVersion)
            || string.IsNullOrWhiteSpace(schemaVersion))
        {
            errors.Add(new ValidationError(offset, partition, "MissingHeader",
                $"Required header '{CustomRequestHeaders.EventSchemaVersion}' is missing."));
        }

        // Validate JSON payload
        var payload = consumeResult.Message.Value;
        var payloadWithinLimit = false;
        if (string.IsNullOrWhiteSpace(payload))
        {
            errors.Add(new ValidationError(offset, partition, "InvalidPayload",
                "Message payload is null or empty."));
        }
        else
        {
            payloadWithinLimit = Encoding.UTF8.GetByteCount(payload) <= _options.Producer.MaxPayloadBytes;
            if (!payloadWithinLimit)
            {
                errors.Add(new ValidationError(offset, partition, "PayloadTooLarge",
                    "Message payload exceeds the configured validation limit."));
            }
            else
            {
                try
                {
                    using var document = JsonDocument.Parse(
                        payload,
                        new JsonDocumentOptions { MaxDepth = 32 });
                }
                catch (JsonException)
                {
                    errors.Add(new ValidationError(offset, partition, "InvalidJson",
                        "Payload is not valid bounded JSON."));
                }
            }
        }

        if (authenticationEnabled)
        {
            if (!headers.TryGetValue(CustomRequestHeaders.MessageSignature, out var signature)
                || string.IsNullOrWhiteSpace(signature))
            {
                errors.Add(new ValidationError(offset, partition, "MissingHeader",
                    $"Required header '{CustomRequestHeaders.MessageSignature}' is missing (authentication is enabled)."));
            }
            else if (hasMessageId
                && hasCorrelationId
                && !string.IsNullOrWhiteSpace(payload)
                && payloadWithinLimit
                && !(topicConfig is null
                    ? _signatureService.VerifySignature(payload, messageId!, correlationId!, signature)
                    : _signatureService.VerifySignature(
                        payload,
                        messageId!,
                        correlationId!,
                        topicConfig.TopicName,
                        consumeResult.Message.Key,
                        schemaVersion ?? string.Empty,
                        KafkaMessageBus.ResolveEnvelopeKind(payload, topicConfig),
                        signature)))
            {
                errors.Add(new ValidationError(offset, partition, "InvalidSignature",
                    "Message signature verification failed."));
            }
        }

        return errors;
    }

    /// <summary>
    /// Resolves bootstrap servers for the given cluster alias.
    /// Falls back to <see cref="KafkaOptions.Servers"/> if the alias is not found.
    /// </summary>
    /// <param name="clusterAlias">The cluster alias to resolve.</param>
    /// <returns>The bootstrap servers connection string.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when no cluster configuration or fallback servers are available.
    /// </exception>
    private string ResolveBootstrapServers(string clusterAlias)
    {
        if (_options.Clusters.TryGetValue(clusterAlias, out var clusterConfig))
        {
            return clusterConfig.BootstrapServers;
        }

        if (!string.IsNullOrWhiteSpace(_options.Servers))
        {
            return _options.Servers;
        }

        throw new InvalidOperationException(
            $"Kafka cluster configuration for '{clusterAlias}' is not defined and no root 'Servers' fallback is configured.");
    }

    /// <summary>
    /// Applies SASL security settings from the cluster configuration to the consumer config.
    /// Topic-level credentials override cluster-level credentials when specified.
    /// </summary>
    /// <param name="consumerConfig">The consumer configuration to update.</param>
    /// <param name="clusterAlias">The resolved cluster alias.</param>
    /// <param name="topicConfig">The topic configuration for credential overrides.</param>
    private void ApplyClusterSecurity(
        ConsumerConfig consumerConfig,
        string clusterAlias,
        KafkaTopicConfiguration topicConfig)
    {
        if (!_options.Clusters.TryGetValue(clusterAlias, out var clusterConfig))
            return;

        if (!string.IsNullOrWhiteSpace(clusterConfig.SecurityProtocol) &&
            Enum.TryParse<SecurityProtocol>(clusterConfig.SecurityProtocol, true, out var protocol))
        {
            consumerConfig.SecurityProtocol = protocol;
        }

        if (!string.IsNullOrWhiteSpace(clusterConfig.SaslMechanism) &&
            Enum.TryParse<SaslMechanism>(clusterConfig.SaslMechanism, true, out var mechanism))
        {
            consumerConfig.SaslMechanism = mechanism;
        }

        // Topic-level credentials override cluster-level
        var effectiveUsername = !string.IsNullOrWhiteSpace(topicConfig.Username)
            ? topicConfig.Username
            : clusterConfig.SaslUsername;
        var effectivePassword = !string.IsNullOrWhiteSpace(topicConfig.Password)
            ? topicConfig.Password
            : clusterConfig.SaslPassword;

        if (!string.IsNullOrWhiteSpace(effectiveUsername) && !string.IsNullOrWhiteSpace(effectivePassword))
        {
            consumerConfig.SaslUsername = effectiveUsername;
            consumerConfig.SaslPassword = effectivePassword;
        }
    }

    /// <summary>
    /// Applies SASL security settings from the cluster configuration to an admin client config.
    /// </summary>
    /// <param name="adminConfig">The admin client configuration to update.</param>
    /// <param name="clusterAlias">The resolved cluster alias.</param>
    private void ApplyClusterSecurityToAdmin(
        AdminClientConfig adminConfig,
        string clusterAlias)
    {
        if (!_options.Clusters.TryGetValue(clusterAlias, out var clusterConfig))
            return;

        if (!string.IsNullOrWhiteSpace(clusterConfig.SecurityProtocol) &&
            Enum.TryParse<SecurityProtocol>(clusterConfig.SecurityProtocol, true, out var protocol))
        {
            adminConfig.SecurityProtocol = protocol;
        }

        if (!string.IsNullOrWhiteSpace(clusterConfig.SaslMechanism) &&
            Enum.TryParse<SaslMechanism>(clusterConfig.SaslMechanism, true, out var mechanism))
        {
            adminConfig.SaslMechanism = mechanism;
        }

        if (!string.IsNullOrWhiteSpace(clusterConfig.SaslUsername) &&
            !string.IsNullOrWhiteSpace(clusterConfig.SaslPassword))
        {
            adminConfig.SaslUsername = clusterConfig.SaslUsername;
            adminConfig.SaslPassword = clusterConfig.SaslPassword;
        }
    }
}
