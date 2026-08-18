namespace CSharpExtensions.Kafka.Core;

using System;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using CSharpExtensions.Kafka.Abstractions;
using System.Collections.Generic;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Validates <see cref="KafkaOptions"/> at application startup.
/// Fails fast if critical configuration is missing or invalid, preventing runtime errors.
/// </summary>
internal sealed class KafkaOptionsValidator : IValidateOptions<KafkaOptions>
{
    private static readonly Regex SegmentRegex = new(@"^[a-z0-9]+(-[a-z0-9]+)*$", RegexOptions.Compiled);
    private static readonly Regex SqlIdentifierRegex = new(
        @"^[A-Za-z_][A-Za-z0-9_]{0,127}$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex S3BucketRegex = new(
        @"^(?!\d{1,3}(?:\.\d{1,3}){3}$)(?!xn--)(?!.*\.\.)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private readonly IRedisConnectionResolver _redisConnectionResolver;
    private readonly CompositeMessageRegistry _compositeRegistry;
    private readonly IReadOnlyList<MessageSubscriptionDescriptor> _subscriptions;
    private readonly ILogger<KafkaOptionsValidator>? _logger;
    public KafkaOptionsValidator(
        IRedisConnectionResolver redisConnectionResolver,
        CompositeMessageRegistry compositeRegistry,
        IReadOnlyList<MessageSubscriptionDescriptor>? subscriptions = null,
        ILogger<KafkaOptionsValidator>? logger = null)
    {
        _redisConnectionResolver = redisConnectionResolver ?? throw new ArgumentNullException(nameof(redisConnectionResolver));
        _compositeRegistry = compositeRegistry ?? throw new ArgumentNullException(nameof(compositeRegistry));
        _subscriptions = subscriptions ?? new List<MessageSubscriptionDescriptor>();
        _logger = logger;
    }

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, KafkaOptions options)
    {
        if (options is null)
        {
            return ValidateOptionsResult.Fail("KafkaOptions is null. Ensure the 'Kafka' section exists in configuration.");
        }

        if (options.Clusters is null
            || options.Topics is null
            || options.Producer is null
            || options.Consumer is null
            || options.Offloading is null
            || options.Outbox is null
            || options.StagedJobs is null
            || options.Idempotency is null
            || options.Assembly is null
            || options.Maintenance is null
            || options.Security is null)
        {
            return ValidateOptionsResult.Fail("Kafka configuration contains a null required section or dictionary.");
        }

        if (!IsSafeName(options.DefaultClusterAlias, 128))
        {
            return ValidateOptionsResult.Fail("Kafka:DefaultClusterAlias is invalid or exceeds 128 characters.");
        }

        // Validate that at least one source of bootstrap servers exists
        var hasRootServers = !string.IsNullOrWhiteSpace(options.Servers);
        var hasClusters = options.Clusters.Count > 0;

        if (!hasRootServers && !hasClusters)
        {
            return ValidateOptionsResult.Fail(
                "No Kafka broker connection configured. Provide either 'Kafka:Servers' or at least one entry in 'Kafka:Clusters'.");
        }

        if (hasRootServers && !IsSafeBootstrapServers(options.Servers))
        {
            return ValidateOptionsResult.Fail("Kafka:Servers contains an invalid broker endpoint list.");
        }

        // Validate cluster configurations
        foreach (var (alias, cluster) in options.Clusters)
        {
            if (!IsSafeName(alias, 128) || cluster is null)
            {
                return ValidateOptionsResult.Fail("Kafka cluster alias or configuration is invalid.");
            }

            if (!IsSafeBootstrapServers(cluster.BootstrapServers))
            {
                return ValidateOptionsResult.Fail(
                    $"Cluster '{alias}' has an invalid BootstrapServers endpoint list.");
            }

            if (!TryParseOptionalEnum<SecurityProtocol>(cluster.SecurityProtocol, out var securityProtocol))
            {
                return ValidateOptionsResult.Fail($"Cluster '{alias}' has an invalid SecurityProtocol value.");
            }

            if (!TryParseOptionalEnum<SaslMechanism>(cluster.SaslMechanism, out var saslMechanism))
            {
                return ValidateOptionsResult.Fail($"Cluster '{alias}' has an invalid SaslMechanism value.");
            }

            if (!HasPairedValues(cluster.SaslUsername, cluster.SaslPassword))
            {
                return ValidateOptionsResult.Fail($"Cluster '{alias}' must provide both SASL username and password.");
            }

            var usesSasl = securityProtocol is SecurityProtocol.SaslPlaintext or SecurityProtocol.SaslSsl;
            if (usesSasl != !string.IsNullOrWhiteSpace(cluster.SaslMechanism))
            {
                return ValidateOptionsResult.Fail(
                    $"Cluster '{alias}' must configure SaslMechanism exactly when a SASL security protocol is used.");
            }

            if (usesSasl
                && saslMechanism is SaslMechanism.Plain or SaslMechanism.ScramSha256 or SaslMechanism.ScramSha512
                && string.IsNullOrWhiteSpace(cluster.SaslUsername))
            {
                return ValidateOptionsResult.Fail($"Cluster '{alias}' requires SASL credentials for the configured mechanism.");
            }

            if (cluster.ReconnectBackoffMs is < 0 or > 600000
                || cluster.ReconnectBackoffMaxMs < cluster.ReconnectBackoffMs
                || cluster.ReconnectBackoffMaxMs > 600000)
            {
                return ValidateOptionsResult.Fail($"Cluster '{alias}' reconnect backoff settings are outside the supported bounds.");
            }
        }

        // Validate topic configurations
        foreach (var (configurationKey, topic) in options.Topics)
        {
            if (!IsSafeName(configurationKey, 256) || topic is null)
            {
                return ValidateOptionsResult.Fail("Kafka topic configuration key or value is invalid.");
            }

            if (!IsSafeKafkaTopicName(topic.TopicName))
            {
                return ValidateOptionsResult.Fail(
                    $"Topic configuration '{configurationKey}' has an invalid physical TopicName. Kafka topic names must be 1-249 ASCII letters, digits, '.', '_' or '-'.");
            }

            if (!string.IsNullOrEmpty(topic.GroupId)
                && (!IsSafeName(topic.GroupId, 255) || topic.GroupId.Contains(':')))
            {
                return ValidateOptionsResult.Fail($"Topic configuration '{configurationKey}' has an invalid consumer GroupId.");
            }

            if (!string.IsNullOrEmpty(topic.Cluster) && !IsSafeName(topic.Cluster, 128))
            {
                return ValidateOptionsResult.Fail($"Topic configuration '{configurationKey}' has an invalid cluster alias.");
            }

            if (!HasPairedValues(topic.Username, topic.Password))
            {
                return ValidateOptionsResult.Fail(
                    $"Topic configuration '{configurationKey}' must provide both topic-level username and password.");
            }

            if (!Enum.IsDefined(topic.Permission))
            {
                return ValidateOptionsResult.Fail($"Topic configuration '{configurationKey}' has an unsupported Permission value.");
            }

            if (topic.MaxDeliveryAttempts is < 1 or > 100)
            {
                return ValidateOptionsResult.Fail(
                    $"Topic configuration '{configurationKey}' must set MaxDeliveryAttempts between 1 and 100.");
            }

            if (topic.IdempotencyRetentionSeconds is < 1 or > 31536000)
            {
                return ValidateOptionsResult.Fail(
                    $"Topic configuration '{configurationKey}' must set IdempotencyRetentionSeconds between 1 and 31536000.");
            }

            if (topic.TopicName.Length > 60)
            {
                _logger?.LogWarning("Topic name '{TopicName}' in configuration '{ConfigurationKey}' exceeds 60 characters (Length: {Length}). Consider shortening it to prevent broker resolution delays.",
                    topic.TopicName, configurationKey, topic.TopicName.Length);
            }

            if (options.StrictTopicNaming)
            {
                // Validate TopicName naming convention
                var cleanTopicName = topic.TopicName;
                if (cleanTopicName.EndsWith(".dlq", StringComparison.OrdinalIgnoreCase))
                {
                    cleanTopicName = cleanTopicName.Substring(0, cleanTopicName.Length - 4);
                }

                var topicSegments = cleanTopicName.Split('.');
                if (topicSegments.Length is not (5 or 6))
                {
                    return ValidateOptionsResult.Fail(
                        $"Topic name '{topic.TopicName}' in configuration '{configurationKey}' must have exactly 5 segments (Isolated Clusters) or 6 segments (Shared Cluster) including version segment. Found {topicSegments.Length} segments.");
                }

                for (int i = 0; i < topicSegments.Length; i++)
                {
                    var segment = topicSegments[i];
                    if (i == topicSegments.Length - 1)
                    {
                        if (string.IsNullOrWhiteSpace(segment) || !Regex.IsMatch(segment, @"^v[1-9][0-9]*$"))
                        {
                            return ValidateOptionsResult.Fail(
                                $"Topic name '{topic.TopicName}' in configuration '{configurationKey}' must end with a valid version segment (e.g. '.v1', '.v2'). Found '{segment}'.");
                        }
                    }
                    else
                    {
                        if (string.IsNullOrWhiteSpace(segment) || !SegmentRegex.IsMatch(segment))
                        {
                            return ValidateOptionsResult.Fail(
                                $"Topic name '{topic.TopicName}' in configuration '{configurationKey}' has an invalid segment '{segment}'. Each segment must contain only lowercase letters, digits, and hyphens, and cannot start/end with a hyphen or contain consecutive hyphens.");
                        }
                    }
                }

                // Validate GroupId naming convention if provided
                if (!string.IsNullOrWhiteSpace(topic.GroupId))
                {
                    if (Regex.IsMatch(topic.GroupId, @"(?i)csharpextensions|wallet"))
                    {
                        return ValidateOptionsResult.Fail(
                            $"GroupId '{topic.GroupId}' in configuration '{configurationKey}' contains prohibited reserved words ('csharpextensions' or 'wallet').");
                    }

                    var groupIdSegments = topic.GroupId.Split('.');
                    if (groupIdSegments.Length is not (4 or 5))
                    {
                        return ValidateOptionsResult.Fail(
                            $"GroupId '{topic.GroupId}' in configuration '{configurationKey}' must have exactly 4 segments (Isolated Clusters) or 5 segments (Shared Cluster). Found {groupIdSegments.Length} segments.");
                    }

                    foreach (var segment in groupIdSegments)
                    {
                        if (string.IsNullOrWhiteSpace(segment) || !SegmentRegex.IsMatch(segment))
                        {
                            return ValidateOptionsResult.Fail(
                                $"GroupId '{topic.GroupId}' in configuration '{configurationKey}' has an invalid segment '{segment}'. Each segment must contain only lowercase letters, digits, and hyphens, and cannot start/end with a hyphen or contain consecutive hyphens.");
                        }
                    }

                    // Check option consistency: Option 1 vs Option 2 (Topic has version segment, GroupId does not)
                    if (topicSegments.Length != groupIdSegments.Length + 1)
                    {
                        return ValidateOptionsResult.Fail(
                            $"Topic name '{topic.TopicName}' ({topicSegments.Length} segments) and GroupId '{topic.GroupId}' ({groupIdSegments.Length} segments) in configuration '{configurationKey}' must use the same cluster strategy option (Topic must have exactly 1 more segment than GroupId).");
                    }

                    if (topicSegments.Length == 6 && !string.Equals(topicSegments[0], groupIdSegments[0], StringComparison.Ordinal))
                    {
                        return ValidateOptionsResult.Fail(
                            $"Topic name '{topic.TopicName}' and GroupId '{topic.GroupId}' in configuration '{configurationKey}' have mismatched environment prefixes ('{topicSegments[0]}' vs '{groupIdSegments[0]}').");
                    }
                }
            }
            else
            {
                _logger?.LogWarning(
                    "Strict topic naming validation is disabled. Permitting legacy topic '{TopicName}' for configuration '{ConfigurationKey}'.",
                    topic.TopicName,
                    configurationKey);
            }

            // Verify cluster reference is resolvable
            var clusterAlias = string.IsNullOrWhiteSpace(topic.Cluster) ? options.DefaultClusterAlias : topic.Cluster;
            if (!options.Clusters.ContainsKey(clusterAlias) && !hasRootServers)
            {
                return ValidateOptionsResult.Fail(
                    $"Topic configuration '{configurationKey}' references cluster '{clusterAlias}' which is not defined in 'Kafka:Clusters', and no root 'Kafka:Servers' fallback is configured.");
            }

            // Check legacy conflict
#pragma warning disable CS0618
            if (topic.EnableOffloading == true && topic.EnableSegmenting == true)
            {
                return ValidateOptionsResult.Fail(
                    $"Topic configuration '{configurationKey}' has both legacy EnableOffloading and EnableSegmenting set to true. " +
                    "These strategies are mutually exclusive. Use 'LargePayloadStrategy' instead.");
            }
#pragma warning restore CS0618

            if (topic.ResolvedStrategy == LargePayloadStrategy.Segmenting)
            {
                return ValidateOptionsResult.Fail(
                    $"Topic configuration '{configurationKey}' enables Segmenting, but the runtime does not implement an authenticated, owner-fenced, durable reassembly protocol. Use S3Offloading or disable the large-payload strategy.");
            }

            if (topic.MaxSegmentSizeBytes is < 1024 or > 100 * 1024 * 1024)
            {
                return ValidateOptionsResult.Fail(
                    $"Topic configuration '{configurationKey}' has MaxSegmentSizeBytes outside 1024-104857600.");
            }

            // Validate idempotency connection alias
            if (topic.IsIdempotent)
            {
                var idempotencyAlias = options.Idempotency.RedisConnectionAlias;
                if (!_redisConnectionResolver.IsRegistered(idempotencyAlias))
                {
                    return ValidateOptionsResult.Fail(
                        $"Topic configuration '{configurationKey}' has idempotency enabled, but the configured Redis connection alias '{idempotencyAlias}' is not registered in the connection resolver.");
                }
            }

            if (topic.EnableDlq)
            {
                var dlqTopic = string.IsNullOrWhiteSpace(topic.TargetDlqTopic)
                    ? $"{topic.TopicName}.dlq"
                    : topic.TargetDlqTopic;
                if (!IsSafeKafkaTopicName(dlqTopic))
                {
                    return ValidateOptionsResult.Fail(
                        $"Topic configuration '{configurationKey}' resolves to an invalid DLQ topic name.");
                }
            }
        }

        // Validate producer settings
        if (options.Producer.MaxRetryCount is < 0 or > 20)
        {
            return ValidateOptionsResult.Fail("Producer.MaxRetryCount must be between 0 and 20.");
        }

        if (options.Producer.RetryBaseDelayMs is < 1 or > 600000
            || options.Producer.MaxRetryDelayMs < options.Producer.RetryBaseDelayMs
            || options.Producer.MaxRetryDelayMs > 600000)
        {
            return ValidateOptionsResult.Fail("Producer retry delays must be within 1-600000ms and ordered.");
        }

        if (!options.Producer.EnableIdempotence && options.Producer.MaxRetryCount > 0)
        {
            return ValidateOptionsResult.Fail(
                "Producer.MaxRetryCount must be 0 when EnableIdempotence is false to avoid ambiguous duplicate publishes.");
        }

        if (options.Producer.LingerMs is < 0 or > 60000
            || options.Producer.MessageTimeoutMs is < 1000 or > 900000
            || options.Producer.QueueBufferingMaxMessages is < 1 or > 10000000)
        {
            return ValidateOptionsResult.Fail("Producer linger, timeout, or queue settings are outside supported bounds.");
        }

        if (options.Producer.MaxPayloadBytes is < 1 or > 100 * 1024 * 1024
            || options.Producer.MaxMessageKeyBytes is < 1 or > 10 * 1024 * 1024)
        {
            return ValidateOptionsResult.Fail("Producer payload or message-key limits are outside supported bounds.");
        }

        if (options.Producer.MaxCachedProducers is < 1 or > 1024)
        {
            return ValidateOptionsResult.Fail("Producer.MaxCachedProducers must be between 1 and 1024.");
        }

        if (!Enum.TryParse<CompressionType>(options.Producer.CompressionType, true, out var compressionType)
            || !Enum.IsDefined(compressionType))
        {
            return ValidateOptionsResult.Fail("Producer.CompressionType is not supported by Confluent.Kafka.");
        }

        // Validate consumer settings
        if (options.Consumer.StartupTimeoutMs is < 1000 or > 120000)
        {
            return ValidateOptionsResult.Fail("Consumer.StartupTimeoutMs must be between 1000 and 120000.");
        }

        if (options.Consumer.SessionTimeoutMs <= 0)
        {
            return ValidateOptionsResult.Fail("Consumer.SessionTimeoutMs must be greater than 0.");
        }

        if (options.Consumer.MaxPollIntervalMs < options.Consumer.SessionTimeoutMs)
        {
            return ValidateOptionsResult.Fail(
                $"Consumer.MaxPollIntervalMs ({options.Consumer.MaxPollIntervalMs}) must be >= Consumer.SessionTimeoutMs ({options.Consumer.SessionTimeoutMs}).");
        }

        if (options.Consumer.SessionTimeoutMs > 300000
            || options.Consumer.MaxPollIntervalMs > 3600000
            || options.Consumer.ConsumeErrorDelayMs is < 1 or > 60000
            || options.Consumer.FetchMinBytes is < 1 or > 100 * 1024 * 1024
            || options.Consumer.FetchMaxWaitMs is < 0 or > 300000
            || options.Consumer.MaxRestartAttempts is < 0 or > 1000
            || options.Consumer.MaxImmediateRetries is < 0 or > 100
            || options.Consumer.CircuitBreakerFailureThreshold is < 1 or > 10000
            || options.Consumer.CircuitBreakerWindowSeconds is < 1 or > 86400
            || options.Consumer.CircuitBreakerCooldownMs is < 1 or > 3600000)
        {
            return ValidateOptionsResult.Fail("Consumer timing, fetch, restart, retry, or circuit-breaker settings are outside supported bounds.");
        }

        if (options.Consumer.RestartBaseDelayMs <= 0
            || options.Consumer.MaxRestartDelayMs < options.Consumer.RestartBaseDelayMs
            || options.Consumer.MaxRestartDelayMs > 600000)
        {
            return ValidateOptionsResult.Fail(
                "Consumer restart delays must be positive and MaxRestartDelayMs must be greater than or equal to RestartBaseDelayMs.");
        }

        if (options.Consumer.HandlerRetryBaseDelayMs <= 0
            || options.Consumer.HandlerRetryMaxDelayMs < options.Consumer.HandlerRetryBaseDelayMs
            || options.Consumer.HandlerRetryMaxDelayMs > 600000)
        {
            return ValidateOptionsResult.Fail(
                "Consumer handler retry delays must be positive and HandlerRetryMaxDelayMs must be greater than or equal to HandlerRetryBaseDelayMs.");
        }

        if (!Enum.IsDefined(options.Maintenance.LockProvider))
        {
            return ValidateOptionsResult.Fail("Maintenance.LockProvider is not a supported lock backend.");
        }

        if (options.Maintenance.IntervalMinutes is < 1 or > 10080
            || options.Maintenance.StaleAssemblyThresholdSeconds is < 60 or > 31536000
            || options.Maintenance.CompletedJobRetentionDays is < 1 or > 3650
            || options.Maintenance.PermanentlyFailedOutboxRetentionDays is < 1 or > 3650
            || options.Maintenance.IndexMaintenanceIntervalHours is < 1 or > 8760
            || options.Maintenance.IndexCommandTimeoutSeconds is < 1 or > 3600)
        {
            return ValidateOptionsResult.Fail("Maintenance timing and retention settings are outside supported bounds.");
        }

        if (options.Maintenance.LockProvider == KafkaMaintenanceLockProvider.SqlServer
            && !IsSafeConnectionName(options.Maintenance.LockConnectionStringName))
        {
            return ValidateOptionsResult.Fail("Maintenance.LockConnectionStringName is invalid for SQL locking.");
        }

        if (!IsSafeName(options.Idempotency.RedisConnectionAlias, 128))
        {
            return ValidateOptionsResult.Fail("Idempotency.RedisConnectionAlias is invalid.");
        }

        // Validate outbox settings
        if (options.Outbox.IsEnabled)
        {
            if (!IsSafeConnectionNameList(options.Outbox.ConnectionStringName)
                || !SqlIdentifierRegex.IsMatch(options.Outbox.TableSchema ?? string.Empty))
            {
                return ValidateOptionsResult.Fail("Outbox connection-string name or SQL schema is invalid.");
            }

            if (options.Outbox.BatchSize is < 1 or > 10000)
            {
                return ValidateOptionsResult.Fail("Outbox.BatchSize must be between 1 and 10000 when outbox is enabled.");
            }

            if (options.Outbox.MaxAttempts is < 1 or > 100)
            {
                return ValidateOptionsResult.Fail("Outbox.MaxAttempts must be between 1 and 100 when outbox is enabled.");
            }

            if (options.Outbox.PollingIntervalMs <= 0)
            {
                return ValidateOptionsResult.Fail("Outbox.PollingIntervalMs must be greater than 0 when outbox is enabled.");
            }

            if (options.Outbox.ErrorDelayMs <= 0)
            {
                return ValidateOptionsResult.Fail("Outbox.ErrorDelayMs must be greater than 0 when outbox is enabled.");
            }

            if (options.Outbox.ProcessingLeaseSeconds is < 30 or > 3600)
            {
                return ValidateOptionsResult.Fail("Outbox.ProcessingLeaseSeconds must be between 30 and 3600 when outbox is enabled.");
            }

            if (options.Outbox.RetryBaseDelaySeconds <= 0
                || options.Outbox.MaxRetryDelaySeconds < options.Outbox.RetryBaseDelaySeconds)
            {
                return ValidateOptionsResult.Fail(
                    "Outbox retry delays must be positive and MaxRetryDelaySeconds must be greater than or equal to RetryBaseDelaySeconds.");
            }
        }

        if (options.StagedJobs.IsEnabled)
        {
            if (!IsSafeConnectionName(options.StagedJobs.ConnectionStringName)
                || !SqlIdentifierRegex.IsMatch(options.StagedJobs.TableSchema ?? string.Empty)
                || options.StagedJobs.BatchSize is < 1 or > 10000
                || options.StagedJobs.PollingIntervalMs is < 1 or > 600000
                || options.StagedJobs.ErrorDelayMs is < 1 or > 600000
                || options.StagedJobs.MaxRetryDelayMs < options.StagedJobs.ErrorDelayMs
                || options.StagedJobs.MaxRetryDelayMs > 3600000
                || options.StagedJobs.MaxAttempts is < 1 or > 100)
            {
                return ValidateOptionsResult.Fail("StagedJobs connection, schema, batch, retry, or timing settings are invalid.");
            }
        }

        if (options.Assembly.IsEnabled)
        {
            if (!Enum.IsDefined(options.Assembly.Provider)
                || options.Assembly.StaleThresholdSeconds is < 60 or > 31536000
                || !SqlIdentifierRegex.IsMatch(options.Assembly.TableSchema ?? string.Empty))
            {
                return ValidateOptionsResult.Fail("Assembly provider, SQL schema, or stale threshold is invalid.");
            }

            if (options.Assembly.Provider == AssemblyProvider.Redis
                && !IsSafeName(options.Assembly.RedisConnectionAlias, 128))
            {
                return ValidateOptionsResult.Fail("Assembly.RedisConnectionAlias is invalid.");
            }

            if (options.Assembly.Provider == AssemblyProvider.SqlServer
                && !IsSafeConnectionName(options.Assembly.ConnectionStringName))
            {
                return ValidateOptionsResult.Fail("Assembly.ConnectionStringName is invalid.");
            }
        }

        if (!Enum.IsDefined(options.Security.SignatureWriteVersion)
            || options.Security.VerificationKeyConfigurationPaths is null)
        {
            return ValidateOptionsResult.Fail("Kafka security signature settings are invalid.");
        }

        if (options.Security.SignatureWriteVersion == KafkaSignatureWriteVersion.LegacyV1
            && !options.Security.AllowLegacyV1Verification
            && options.Topics.Values.Any(topic => topic.EnableAuthentication))
        {
            return ValidateOptionsResult.Fail(
                "LegacyV1 cannot be used for authenticated writes while AllowLegacyV1Verification is false.");
        }

        foreach (var (verificationKeyId, configurationPath) in options.Security.VerificationKeyConfigurationPaths)
        {
            try
            {
                SignatureService.ValidateKeyId(verificationKeyId);
            }
            catch (InvalidOperationException)
            {
                return ValidateOptionsResult.Fail("A Kafka verification key ID is invalid.");
            }

            if (!IsSafeConfigurationPath(configurationPath))
            {
                return ValidateOptionsResult.Fail(
                    $"Kafka verification-key configuration path for '{verificationKeyId}' is invalid.");
            }
        }

        if (options.Security.SignatureWriteVersion == KafkaSignatureWriteVersion.HmacSha256V2)
        {
            var keyPath = options.Security.SignatureKeyConfigurationPath;
            if (!IsSafeConfigurationPath(keyPath))
            {
                return ValidateOptionsResult.Fail("Kafka HMAC v2 requires a signature key configuration path.");
            }

            try
            {
                SignatureService.ValidateKeyId(options.Security.SignatureKeyId);
            }
            catch (InvalidOperationException)
            {
                return ValidateOptionsResult.Fail("Kafka:Security:SignatureKeyId is invalid.");
            }
        }

        // Validate S3 Offloading configuration only if explicitly requested
#pragma warning disable CS0618
        var hasExplicitOffloading = options.Topics.Values.Any(topic => 
            topic.LargePayloadStrategy == LargePayloadStrategy.S3Offloading || 
            topic.EnableOffloading == true);
#pragma warning restore CS0618

        if (hasExplicitOffloading)
        {
            if (!IsSafeS3Bucket(options.Offloading.BucketName))
            {
                return ValidateOptionsResult.Fail("Kafka:Offloading:BucketName is not a valid S3 bucket name.");
            }

            if (!IsSafeAwsRegion(options.Offloading.Region))
            {
                return ValidateOptionsResult.Fail("Kafka:Offloading:Region is invalid.");
            }

            if (options.Offloading.InlineThresholdBytes <= 0)
            {
                return ValidateOptionsResult.Fail("Kafka:Offloading:InlineThresholdBytes must be greater than 0.");
            }

            if (options.Offloading.MaxDownloadBytes < options.Offloading.InlineThresholdBytes
                || options.Offloading.MaxDownloadBytes > 256 * 1024 * 1024)
            {
                return ValidateOptionsResult.Fail(
                    "Kafka:Offloading:MaxDownloadBytes must be greater than or equal to InlineThresholdBytes and no greater than 256 MiB.");
            }

            if (options.Offloading.SkipHashVerification)
            {
                return ValidateOptionsResult.Fail(
                    "Kafka:Offloading:SkipHashVerification cannot be enabled because claim-check payload integrity is mandatory.");
            }

            var keyPrefix = options.Offloading.KeyPrefix ?? string.Empty;
            if (keyPrefix.Contains("..", StringComparison.Ordinal)
                || keyPrefix.Contains('\\')
                || keyPrefix.StartsWith("/", StringComparison.Ordinal)
                || !string.Equals(keyPrefix, keyPrefix.Trim(), StringComparison.Ordinal)
                || ContainsControlCharacter(keyPrefix)
                || Encoding.UTF8.GetByteCount(keyPrefix) > 900)
            {
                return ValidateOptionsResult.Fail(
                    "Kafka:Offloading:KeyPrefix must be a normalized relative S3 prefix without traversal, backslashes, or surrounding whitespace.");
            }

            if (options.Offloading.RetentionDays is < 1 or > 3650)
            {
                return ValidateOptionsResult.Fail("Kafka:Offloading:RetentionDays must be between 1 and 3650.");
            }

            if (!Enum.IsDefined(options.Offloading.ExpirationStrategy)
                || !Enum.IsDefined(options.Offloading.ServerSideEncryption))
            {
                return ValidateOptionsResult.Fail("Kafka:Offloading contains an unsupported expiration or encryption policy.");
            }

            var hasKmsKey = !string.IsNullOrWhiteSpace(options.Offloading.KmsKeyId);
            if ((options.Offloading.ServerSideEncryption == S3ServerSideEncryptionPolicy.Kms) != hasKmsKey
                || (hasKmsKey
                    && (options.Offloading.KmsKeyId.Length > 2048
                        || ContainsControlCharacter(options.Offloading.KmsKeyId))))
            {
                return ValidateOptionsResult.Fail(
                    "Kafka:Offloading:KmsKeyId must be set only for Kms encryption and must be a safe bounded identifier.");
            }

            if (options.Offloading.CustomObjectTags is null)
            {
                return ValidateOptionsResult.Fail("Kafka:Offloading:CustomObjectTags cannot be null.");
            }

            var lifecycleTagCount = options.Offloading.ExpirationStrategy == S3ExpirationStrategy.ObjectTagging ? 1 : 0;
            if (options.Offloading.CustomObjectTags.Count + lifecycleTagCount > 10)
            {
                return ValidateOptionsResult.Fail("Kafka:Offloading supports at most 10 S3 object tags including the lifecycle tag.");
            }

            if (options.Offloading.ExpirationStrategy == S3ExpirationStrategy.ObjectTagging
                && !IsSafeS3Tag(options.Offloading.LifecycleTagName, 128, allowEmpty: false))
            {
                return ValidateOptionsResult.Fail("Kafka:Offloading:LifecycleTagName is invalid.");
            }

            foreach (var (tagKey, tagValue) in options.Offloading.CustomObjectTags)
            {
                if (!IsSafeS3Tag(tagKey, 128, allowEmpty: false)
                    || !IsSafeS3Tag(tagValue, 256, allowEmpty: true)
                    || (options.Offloading.ExpirationStrategy == S3ExpirationStrategy.ObjectTagging
                        && string.Equals(tagKey, options.Offloading.LifecycleTagName, StringComparison.Ordinal)))
                {
                    return ValidateOptionsResult.Fail("Kafka:Offloading contains an invalid or duplicate S3 object tag.");
                }
            }
        }

        if (_compositeRegistry.GetAllBuilders().Count > 0)
        {
            return ValidateOptionsResult.Fail(
                "Kafka composite aggregation is not available because its persistence providers do not yet " +
                "implement an owner-fenced atomic state transition and completion protocol.");
        }

        foreach (var descriptor in _subscriptions)
        {
            var configKey = TopicAttributeResolver.Resolve(descriptor.MessageType);
            if (options.Topics.TryGetValue(configKey, out var topicConfig))
            {
                var derivedTopicName = TopicAttributeResolver.ResolveTopicName(descriptor.MessageType);
                if (!string.Equals(topicConfig.TopicName, derivedTopicName, StringComparison.Ordinal))
                {
                    _logger?.LogWarning("Topic name mismatch for message '{MessageType}' (ConfigKey: '{ConfigKey}'). Configured physical name is '{ConfiguredName}', but semantic convention derives '{DerivedName}'.",
                        descriptor.MessageType.FullName, configKey, topicConfig.TopicName, derivedTopicName);
                }


                if (descriptor.Options.ReadMode == KafkaReadMode.HistoricalReplay)
                {
                    if (string.IsNullOrWhiteSpace(descriptor.Options.ConsumerGroup))
                    {
                        return ValidateOptionsResult.Fail(
                            $"Historical replay subscription for '{descriptor.MessageType.FullName}' requires an explicit isolated ConsumerGroup override.");
                    }

                    if (!string.IsNullOrWhiteSpace(topicConfig.GroupId)
                        && string.Equals(descriptor.Options.ConsumerGroup, topicConfig.GroupId, StringComparison.Ordinal))
                    {
                        return ValidateOptionsResult.Fail(
                            $"Historical replay subscription for '{descriptor.MessageType.FullName}' cannot use the live consumer group '{topicConfig.GroupId}'.");
                    }

                    if (!KafkaMessageBus.TryParseHistoricalReplayStart(descriptor.Options.StartOffsetTime, out _, out _))
                    {
                        return ValidateOptionsResult.Fail(
                            $"Historical replay subscription for '{descriptor.MessageType.FullName}' requires a non-negative offset or an ISO 8601 timestamp.");
                    }
                }
            }
        }

        return ValidateOptionsResult.Success;
    }

    private static bool IsSafeName(string? value, int maxLength)
    {
        return !string.IsNullOrWhiteSpace(value)
            && value.Length <= maxLength
            && !ContainsControlCharacter(value);
    }

    private static bool IsSafeConnectionName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 256)
        {
            return false;
        }

        foreach (var character in value)
        {
            if (!(char.IsAsciiLetterOrDigit(character) || character is '.' or '_' or '-'))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsSafeConnectionNameList(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var names = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return names.Length > 0
            && names.Length <= 128
            && names.All(name => IsSafeConnectionName(name) || IsSafeConfigurationPath(name))
            && names.Distinct(StringComparer.Ordinal).Count() == names.Length;
    }

    private static bool IsSafeConfigurationPath(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 512)
        {
            return false;
        }

        foreach (var character in value)
        {
            if (!(char.IsAsciiLetterOrDigit(character) || character is ':' or '.' or '_' or '-'))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsSafeBootstrapServers(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)
            || value.Length > 4096
            || ContainsControlCharacter(value))
        {
            return false;
        }

        var endpoints = value.Split(',', StringSplitOptions.None);
        if (endpoints.Length is < 1 or > 64)
        {
            return false;
        }

        foreach (var endpoint in endpoints)
        {
            if (string.IsNullOrWhiteSpace(endpoint)
                || endpoint.Length > 512
                || !string.Equals(endpoint, endpoint.Trim(), StringComparison.Ordinal)
                || endpoint.Any(char.IsWhiteSpace))
            {
                return false;
            }

            string host;
            string? port = null;
            if (endpoint[0] == '[')
            {
                var bracket = endpoint.IndexOf(']');
                if (bracket <= 1)
                {
                    return false;
                }

                host = endpoint[1..bracket];
                if (bracket + 1 < endpoint.Length)
                {
                    if (endpoint[bracket + 1] != ':') return false;
                    port = endpoint[(bracket + 2)..];
                }
            }
            else
            {
                var colon = endpoint.LastIndexOf(':');
                if (colon >= 0)
                {
                    if (endpoint.IndexOf(':') != colon) return false;
                    host = endpoint[..colon];
                    port = endpoint[(colon + 1)..];
                }
                else
                {
                    host = endpoint;
                }
            }

            if (string.IsNullOrWhiteSpace(host)
                || host.Any(character => char.IsControl(character) || char.IsWhiteSpace(character))
                || host.IndexOfAny(['/', '\\', '?', '#', ',']) >= 0)
            {
                return false;
            }

            if (port is not null
                && (!int.TryParse(port, out var portNumber) || portNumber is < 1 or > 65535))
            {
                return false;
            }
        }

        return true;
    }

    private static bool TryParseOptionalEnum<TEnum>(string? value, out TEnum parsed)
        where TEnum : struct, Enum
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            parsed = default;
            return true;
        }

        return Enum.TryParse(value, ignoreCase: true, out parsed) && Enum.IsDefined(parsed);
    }

    private static bool HasPairedValues(string? first, string? second)
    {
        return string.IsNullOrWhiteSpace(first) == string.IsNullOrWhiteSpace(second)
            && (string.IsNullOrEmpty(first) || first.Length <= 1024)
            && (string.IsNullOrEmpty(second) || second.Length <= 4096);
    }

    private static bool IsSafeKafkaTopicName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 249 || value is "." or "..")
        {
            return false;
        }

        foreach (var character in value)
        {
            if (!(char.IsAsciiLetterOrDigit(character) || character is '.' or '_' or '-'))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsSafeS3Bucket(string? value)
    {
        return !string.IsNullOrWhiteSpace(value)
            && S3BucketRegex.IsMatch(value)
            && !value.Contains(".-", StringComparison.Ordinal)
            && !value.Contains("-.", StringComparison.Ordinal);
    }

    private static bool IsSafeAwsRegion(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length is < 3 or > 64)
        {
            return false;
        }

        if (value[0] == '-' || value[^1] == '-') return false;
        return value.All(character => character is >= 'a' and <= 'z' || char.IsAsciiDigit(character) || character == '-');
    }

    private static bool IsSafeS3Tag(string? value, int maxLength, bool allowEmpty)
    {
        if (value is null || value.Length > maxLength || (!allowEmpty && value.Length == 0))
        {
            return false;
        }

        foreach (var character in value)
        {
            if (!(char.IsLetterOrDigit(character)
                  || character is ' ' or '+' or '-' or '=' or '.' or '_' or ':' or '/' or '@'))
            {
                return false;
            }
        }

        return true;
    }

    private static bool ContainsControlCharacter(string? value)
    {
        return value is not null && value.Any(char.IsControl);
    }
}
