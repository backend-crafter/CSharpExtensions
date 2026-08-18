using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Manages and caches physical Kafka producers for multi-cluster routing.
/// Supports application-level retry for transient client errors and cluster-level SASL fallback.
/// </summary>
public sealed class KafkaProducerManager : IDisposable
{
    private const int MaxHeaderCount = 64;
    private const int MaxHeaderKeyBytes = 128;
    private const int MaxHeaderValueBytes = 8 * 1024;
    private const int MaxTotalHeaderBytes = 64 * 1024;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaProducerManager> _logger;
    private readonly Func<ProducerConfig, IProducer<string, string>> _producerFactory;
    private readonly ConcurrentDictionary<ProducerCacheKey, Lazy<IProducer<string, string>>> _producers = new();
    private readonly byte[] _credentialFingerprintKey = RandomNumberGenerator.GetBytes(32);
    private readonly object _producerCacheGate = new();
    private readonly object _lifetimeSync = new();
    private int _activePublishes;
    private bool _disposed;
    private bool _cleanupStarted;

    public KafkaProducerManager(IOptions<KafkaOptions> options, ILogger<KafkaProducerManager> logger)
        : this(
            options,
            logger,
            config => new ProducerBuilder<string, string>(config).Build())
    {
    }

    internal KafkaProducerManager(
        IOptions<KafkaOptions> options,
        ILogger<KafkaProducerManager> logger,
        Func<ProducerConfig, IProducer<string, string>> producerFactory)
    {
        if (options is null) throw new ArgumentNullException(nameof(options));
        _options = options.Value;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _producerFactory = producerFactory ?? throw new ArgumentNullException(nameof(producerFactory));
    }

    internal int CachedProducerCount => _producers.Count;

    /// <summary>
    /// Publishes a raw message payload directly to the specified Kafka cluster and topic.
    /// Includes application-level retry with exponential backoff for transient client errors.
    /// </summary>
    public async Task<Result> PublishDirectAsync(
        string topicName,
        string clusterAlias,
        string? messageKey,
        string payloadJson,
        Dictionary<string, string> headers,
        string? username = null,
        string? password = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName)) throw new ArgumentException("Topic name cannot be empty.", nameof(topicName));
        if (string.IsNullOrWhiteSpace(clusterAlias)) throw new ArgumentException("Cluster alias cannot be empty.", nameof(clusterAlias));
        if (payloadJson is null) throw new ArgumentNullException(nameof(payloadJson));
        if (headers is null) throw new ArgumentNullException(nameof(headers));

        cancellationToken.ThrowIfCancellationRequested();
        ValidatePublishInput(topicName, clusterAlias, messageKey, payloadJson, headers);
        EnterPublish();

        try
        {
            var maxRetries = _options.Producer.MaxRetryCount;
            for (var attempt = 0; attempt <= maxRetries; attempt++)
            {
                try
                {
                    var producer = GetOrCreateProducer(clusterAlias, username, password);
                    var kafkaMessage = new Message<string, string>
                    {
                        Key = _options.Producer.PreserveNullMessageKey ? messageKey! : messageKey ?? string.Empty,
                        Value = payloadJson,
                        Headers = new Headers()
                    };

                    foreach (var header in headers)
                    {
                        kafkaMessage.Headers.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
                    }

                    var deliveryReport = await producer.ProduceAsync(topicName, kafkaMessage, cancellationToken);
                    if (deliveryReport.Status == PersistenceStatus.Persisted)
                    {
                        return Result.Success();
                    }

                    return Result.Failure("Kafka broker did not confirm message persistence.");
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (ProduceException<string, string> exception)
                    when (!exception.Error.IsFatal && IsTransientClientError(exception) && attempt < maxRetries)
                {
                    var delay = CalculateRetryDelay(
                        attempt,
                        _options.Producer.RetryBaseDelayMs,
                        _options.Producer.MaxRetryDelayMs);

                    _logger.LogWarning(
                        "Transient producer error on topic '{TopicName}' (attempt {Attempt}/{MaxRetries}, ErrorCode: {ErrorCode}). Retrying in {DelayMs}ms.",
                        topicName, attempt + 1, maxRetries, exception.Error.Code, delay);

                    await Task.Delay(delay, cancellationToken);
                }
                catch (ProduceException<string, string> exception)
                {
                    _logger.LogError(
                        "Producer error on topic '{TopicName}' after {Attempts} attempt(s). ErrorCode: {ErrorCode}, IsFatal: {IsFatal}.",
                        topicName, attempt + 1, exception.Error.Code, exception.Error.IsFatal);

                    return Result.Failure("Kafka producer rejected the publish operation.");
                }
                catch (Exception exception)
                {
                    _logger.LogError(
                        "Kafka producer failed for topic '{TopicName}'. ErrorType: {ErrorType}.",
                        topicName,
                        exception.GetType().Name);
                    return Result.Failure($"Kafka producer failed with error type '{exception.GetType().Name}'.");
                }
            }

            return Result.Failure("Kafka producer exhausted all retry attempts.");
        }
        finally
        {
            ExitPublish();
        }
    }

    internal IProducer<string, string> GetOrCreateProducer(
        string clusterAlias,
        string? username,
        string? password)
    {
        var resolvedAlias = string.IsNullOrWhiteSpace(clusterAlias) ? _options.DefaultClusterAlias : clusterAlias;
        if (!_options.Clusters.TryGetValue(resolvedAlias, out var clusterConfig))
        {
            if (!string.IsNullOrWhiteSpace(_options.Servers))
            {
                clusterConfig = new KafkaClusterConfiguration { BootstrapServers = _options.Servers };
            }
            else
            {
                throw new InvalidOperationException($"Kafka cluster configuration for '{resolvedAlias}' is not defined and no root 'Servers' fallback is configured.");
            }
        }

        var (effectiveUsername, effectivePassword) = ResolveCredentials(
            clusterConfig,
            username,
            password);
        var producerKey = new ProducerCacheKey(
            resolvedAlias,
            CreateCredentialFingerprint(effectiveUsername, effectivePassword));
        if (!_producers.TryGetValue(producerKey, out var lazyProducer))
        {
            lock (_producerCacheGate)
            {
                if (!_producers.TryGetValue(producerKey, out lazyProducer))
                {
                    if (_producers.Count >= _options.Producer.MaxCachedProducers)
                    {
                        throw new InvalidOperationException(
                            "Kafka producer cache capacity has been reached. Configure a bounded set of cluster and credential combinations or increase Producer.MaxCachedProducers.");
                    }

                    lazyProducer = new Lazy<IProducer<string, string>>(
                        () => BuildProducer(
                            resolvedAlias,
                            clusterConfig,
                            effectiveUsername,
                            effectivePassword),
                        LazyThreadSafetyMode.ExecutionAndPublication);
                    if (!_producers.TryAdd(producerKey, lazyProducer))
                    {
                        throw new InvalidOperationException("Kafka producer cache admission failed unexpectedly.");
                    }
                }
            }
        }

        try
        {
            return lazyProducer.Value;
        }
        catch
        {
            lock (_producerCacheGate)
            {
                ((ICollection<KeyValuePair<ProducerCacheKey, Lazy<IProducer<string, string>>>>)_producers)
                    .Remove(new KeyValuePair<ProducerCacheKey, Lazy<IProducer<string, string>>>(producerKey, lazyProducer));
            }
            throw;
        }
    }

    private IProducer<string, string> BuildProducer(
        string resolvedAlias,
        KafkaClusterConfiguration clusterConfig,
        string? effectiveUsername,
        string? effectivePassword)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = clusterConfig.BootstrapServers,
            AllowAutoCreateTopics = false,
            Acks = Acks.All,
            MessageTimeoutMs = _options.Producer.MessageTimeoutMs,
            LingerMs = _options.Producer.LingerMs,
            EnableIdempotence = _options.Producer.EnableIdempotence,
            QueueBufferingMaxMessages = _options.Producer.QueueBufferingMaxMessages
        };

        if (!string.IsNullOrWhiteSpace(_options.Producer.CompressionType) &&
            Enum.TryParse<CompressionType>(_options.Producer.CompressionType, true, out var compressionType))
        {
            config.CompressionType = compressionType;
        }

        config.ReconnectBackoffMs = clusterConfig.ReconnectBackoffMs;
        config.ReconnectBackoffMaxMs = clusterConfig.ReconnectBackoffMaxMs;

            if (!string.IsNullOrWhiteSpace(clusterConfig.SecurityProtocol))
            {
                if (Enum.TryParse<SecurityProtocol>(clusterConfig.SecurityProtocol, true, out var protocol))
                {
                    config.SecurityProtocol = protocol;
                }
                else
                {
                    throw new ArgumentException($"Invalid SecurityProtocol value '{clusterConfig.SecurityProtocol}' configured for cluster '{resolvedAlias}'.");
                }
            }

            if (!string.IsNullOrWhiteSpace(clusterConfig.SaslMechanism))
            {
                if (Enum.TryParse<SaslMechanism>(clusterConfig.SaslMechanism, true, out var mechanism))
                {
                    config.SaslMechanism = mechanism;
                }
                else
                {
                    throw new ArgumentException($"Invalid SaslMechanism value '{clusterConfig.SaslMechanism}' configured for cluster '{resolvedAlias}'.");
                }
            }

        if (!string.IsNullOrWhiteSpace(effectiveUsername) && !string.IsNullOrWhiteSpace(effectivePassword))
        {
            config.SaslUsername = effectiveUsername;
            config.SaslPassword = effectivePassword;
        }

        return _producerFactory(config);
    }

    /// <summary>
    /// Determines whether a produce exception represents a transient client-side error
    /// that librdkafka does not automatically retry.
    /// </summary>
    private static bool IsTransientClientError(ProduceException<string, string> exception)
    {
        return exception.Error.Code switch
        {
            ErrorCode.Local_QueueFull => true,
            ErrorCode.Local_Transport => true,
            ErrorCode.Local_TimedOut => true,
            ErrorCode.Local_MsgTimedOut => true,
            ErrorCode.Local_AllBrokersDown => true,
            _ => false
        };
    }

    private static (string? Username, string? Password) ResolveCredentials(
        KafkaClusterConfiguration clusterConfig,
        string? username,
        string? password)
    {
        var hasTopicUsername = !string.IsNullOrWhiteSpace(username);
        var hasTopicPassword = !string.IsNullOrWhiteSpace(password);
        if (hasTopicUsername != hasTopicPassword)
        {
            throw new InvalidOperationException("Kafka topic-level credentials must provide both username and password.");
        }

        if (hasTopicUsername)
        {
            return (username, password);
        }

        var hasClusterUsername = !string.IsNullOrWhiteSpace(clusterConfig.SaslUsername);
        var hasClusterPassword = !string.IsNullOrWhiteSpace(clusterConfig.SaslPassword);
        if (hasClusterUsername != hasClusterPassword)
        {
            throw new InvalidOperationException("Kafka cluster credentials must provide both username and password.");
        }

        return hasClusterUsername
            ? (clusterConfig.SaslUsername, clusterConfig.SaslPassword)
            : (null, null);
    }

    private string CreateCredentialFingerprint(string? username, string? password)
    {
        byte[]? usernameBytes = null;
        byte[]? passwordBytes = null;
        byte[]? fingerprint = null;
        try
        {
            usernameBytes = StrictUtf8.GetBytes(username ?? string.Empty);
            passwordBytes = StrictUtf8.GetBytes(password ?? string.Empty);
            using var hash = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, _credentialFingerprintKey);
            Span<byte> lengthPrefix = stackalloc byte[sizeof(int)];
            BinaryPrimitives.WriteInt32BigEndian(lengthPrefix, usernameBytes.Length);
            hash.AppendData(lengthPrefix);
            hash.AppendData(usernameBytes);
            BinaryPrimitives.WriteInt32BigEndian(lengthPrefix, passwordBytes.Length);
            hash.AppendData(lengthPrefix);
            hash.AppendData(passwordBytes);
            fingerprint = hash.GetHashAndReset();
            return Convert.ToHexString(fingerprint);
        }
        finally
        {
            if (usernameBytes is not null) CryptographicOperations.ZeroMemory(usernameBytes);
            if (passwordBytes is not null) CryptographicOperations.ZeroMemory(passwordBytes);
            if (fingerprint is not null) CryptographicOperations.ZeroMemory(fingerprint);
        }
    }

    private void ValidatePublishInput(
        string topicName,
        string clusterAlias,
        string? messageKey,
        string payloadJson,
        Dictionary<string, string> headers)
    {
        if (!IsSafeKafkaTopicName(topicName))
        {
            throw new ArgumentException("Kafka topic name is invalid.", nameof(topicName));
        }

        if (clusterAlias.Length > 128 || ContainsControlCharacter(clusterAlias))
        {
            throw new ArgumentException("Kafka cluster alias is invalid.", nameof(clusterAlias));
        }

        if (StrictUtf8.GetByteCount(payloadJson) > _options.Producer.MaxPayloadBytes)
        {
            throw new ArgumentException("Kafka payload exceeds the configured producer limit.", nameof(payloadJson));
        }

        if (messageKey is not null && StrictUtf8.GetByteCount(messageKey) > _options.Producer.MaxMessageKeyBytes)
        {
            throw new ArgumentException("Kafka message key exceeds the configured producer limit.", nameof(messageKey));
        }

        if (headers.Count > MaxHeaderCount)
        {
            throw new ArgumentException("Kafka header count exceeds the permitted limit.", nameof(headers));
        }

        var totalBytes = 0;
        foreach (var header in headers)
        {
            if (!IsSafeHeaderKey(header.Key))
            {
                throw new ArgumentException("Kafka header key is invalid.", nameof(headers));
            }

            if (header.Value is null)
            {
                throw new ArgumentException("Kafka header value cannot be null.", nameof(headers));
            }

            var keyBytes = StrictUtf8.GetByteCount(header.Key);
            var valueBytes = StrictUtf8.GetByteCount(header.Value);
            if (keyBytes > MaxHeaderKeyBytes || valueBytes > MaxHeaderValueBytes)
            {
                throw new ArgumentException("Kafka header exceeds the permitted size.", nameof(headers));
            }

            totalBytes = checked(totalBytes + keyBytes + valueBytes);
            if (totalBytes > MaxTotalHeaderBytes)
            {
                throw new ArgumentException("Kafka headers exceed the permitted total size.", nameof(headers));
            }
        }
    }

    private static bool IsSafeKafkaTopicName(string value)
    {
        if (value.Length is < 1 or > 249 || value is "." or "..")
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

    private static bool IsSafeHeaderKey(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        foreach (var character in value)
        {
            if (!(char.IsAsciiLetterOrDigit(character) || character is '-' or '_' or '.'))
            {
                return false;
            }
        }

        return true;
    }

    private static bool ContainsControlCharacter(string value)
    {
        foreach (var character in value)
        {
            if (char.IsControl(character))
            {
                return true;
            }
        }

        return false;
    }

    internal static int CalculateRetryDelay(int attempt, int baseDelayMs, int maxDelayMs)
    {
        if (baseDelayMs <= 0 || maxDelayMs < baseDelayMs)
        {
            throw new ArgumentOutOfRangeException(nameof(baseDelayMs));
        }

        var delay = baseDelayMs;
        for (var index = 0; index < attempt && delay < maxDelayMs; index++)
        {
            delay = (int)Math.Min((long)maxDelayMs, (long)delay * 2);
        }

        return delay;
    }

    private void EnterPublish()
    {
        lock (_lifetimeSync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _activePublishes++;
        }
    }

    private void ExitPublish()
    {
        var cleanup = false;
        lock (_lifetimeSync)
        {
            _activePublishes--;
            if (_disposed && _activePublishes == 0 && !_cleanupStarted)
            {
                _cleanupStarted = true;
                cleanup = true;
            }
        }

        if (cleanup)
        {
            CleanupProducers();
        }
    }

    public void Dispose()
    {
        var cleanup = false;
        lock (_lifetimeSync)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (_activePublishes == 0)
            {
                _cleanupStarted = true;
                cleanup = true;
            }
        }

        if (cleanup)
        {
            CleanupProducers();
        }
    }

    private void CleanupProducers()
    {
        foreach (var lazyProducer in _producers.Values)
        {
            if (!lazyProducer.IsValueCreated)
            {
                continue;
            }

            try
            {
                var producer = lazyProducer.Value;
                producer.Flush(TimeSpan.FromSeconds(5));
                producer.Dispose();
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    "Kafka producer cleanup failed. ErrorType: {ErrorType}.",
                    exception.GetType().Name);
            }
        }

        _producers.Clear();
        CryptographicOperations.ZeroMemory(_credentialFingerprintKey);
    }

    private readonly record struct ProducerCacheKey(string ClusterAlias, string CredentialFingerprint);
}
