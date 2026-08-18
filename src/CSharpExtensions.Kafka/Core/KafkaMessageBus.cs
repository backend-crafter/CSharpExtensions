using CSharpExtensions.Foundation.Helpers.Constants;
using CSharpExtensions.Foundation.Json;
using CSharpExtensions.Foundation.Railway;
using CSharpExtensions.Foundation.Railway.Extensions;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Evolution;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using RailwayError = Error;

/// <summary>
/// Core implementation of the Kafka Message Bus, tying together publishing, subscriptions, 
/// claim check offloading, distributed idempotency, cryptographic signatures, upcasting, and telemetry.
/// Implements <see cref="IAsyncDisposable"/> for graceful consumer shutdown.
/// </summary>
public sealed class KafkaMessageBus : IMessageBus, IAsyncDisposable
{
    private static readonly ActivitySource ActivitySource = new("CSharpExtensions.Kafka");
    private const int MaxClaimCheckEnvelopeBytes = 16 * 1024;
    private const int MaxClaimCheckEnvelopeDepth = 16;

    private readonly IServiceProvider _serviceProvider;
    private readonly KafkaProducerManager _producerManager;
    private readonly IDistributedDuplicateDetector _duplicateDetector;
    private readonly SignatureService _signatureService;
    private readonly S3ClaimCheckOffloader _offloader;
    private readonly MessageUpcastRegistry _upcastRegistry;
    private readonly IKafkaMetricsCollector? _metricsCollector;
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaMessageBus> _logger;
    private readonly ConcurrentDictionary<ConsumerTaskKey, Task> _consumerTasks = new();
    private readonly CancellationTokenSource _shutdownCancellation = new();
    private int _disposed;

    internal readonly record struct ConsumerTaskKey(string TopicName, string ConsumerGroup);

    public KafkaMessageBus(
        IServiceProvider serviceProvider,
        KafkaProducerManager producerManager,
        IDistributedDuplicateDetector duplicateDetector,
        SignatureService signatureService,
        S3ClaimCheckOffloader offloader,
        MessageUpcastRegistry upcastRegistry,
        IOptions<KafkaOptions> options,
        ILogger<KafkaMessageBus> logger,
        IKafkaMetricsCollector? metricsCollector = null)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _producerManager = producerManager ?? throw new ArgumentNullException(nameof(producerManager));
        _duplicateDetector = duplicateDetector ?? throw new ArgumentNullException(nameof(duplicateDetector));
        _signatureService = signatureService ?? throw new ArgumentNullException(nameof(signatureService));
        _offloader = offloader ?? throw new ArgumentNullException(nameof(offloader));
        _upcastRegistry = upcastRegistry ?? throw new ArgumentNullException(nameof(upcastRegistry));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _metricsCollector = metricsCollector;
    }

    /// <summary>
    /// Resolves the configuration key for a given message type.
    /// Uses the [Topic] attribute's ConfigurationKey if present, otherwise falls back to the C# class name.
    /// </summary>
    private static string ResolveConfigurationKey<TMessage>()
    {
        return TopicAttributeResolver.Resolve<TMessage>();
    }

    /// <inheritdoc />
    public async Task<Result> PublishAsync<TMessage>(
        TMessage message,
        string? messageKey = null,
        CancellationToken cancellationToken = default) where TMessage : class
    {
        if (message is null) throw new ArgumentNullException(nameof(message));
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        using var activity = KafkaDiagnostics.ActivitySource.StartActivity("CSharpExtensions.Kafka.publish");
        activity?.SetTag("messaging.system", "kafka");

        try
        {
            var configurationKey = ResolveConfigurationKey<TMessage>();
            if (!_options.Topics.TryGetValue(configurationKey, out var topicConfig))
            {
                return Result.Failure($"Kafka configuration key '{configurationKey}' is not defined.");
            }

            // Enforce write permissions
            if (topicConfig.Permission != TopicPermission.Write && topicConfig.Permission != TopicPermission.ReadWrite)
            {
                throw new SecurityException($"Unauthorized publish attempt. Write access is restricted on topic configuration '{configurationKey}'.");
            }

            // Validate message version matches topic version suffix
            var topicSegments = topicConfig.TopicName.Split('.');
            var topicVersionSegment = topicSegments.Last();
            if (topicVersionSegment.StartsWith("v") && int.TryParse(topicVersionSegment.Substring(1), out var topicVersion))
            {
                var messageVersion = MessageVersionResolver.GetMessageVersion(message);
                if (messageVersion != topicVersion)
                {
                    return Result.Failure($"Version mismatch: Message version ({messageVersion}) does not match topic version suffix ({topicVersionSegment}) for topic '{topicConfig.TopicName}'.");
                }
            }

            activity?.SetTag("messaging.destination", topicConfig.TopicName);

            var rawPayloadJson = JsonSerializer.Serialize(message, JsonOptions.KafkaCompatible);
            var finalPayload = rawPayloadJson;
            var isOffloadedReference = false;
            var resolvedStrategy = topicConfig.ResolvedStrategy;
            var byteCount = Encoding.UTF8.GetByteCount(rawPayloadJson);

            // 1. Large Payload Strategy handling
            if (resolvedStrategy == LargePayloadStrategy.S3Offloading 
                && byteCount > _options.Offloading.InlineThresholdBytes)
            {
                var offloadResult = await _offloader.OffloadAsync(
                    rawPayloadJson,
                    configurationKey,
                    _options.Offloading,
                    cancellationToken);

                if (!offloadResult.IsSuccess)
                {
                    return Result.Failure("S3 payload offload failed.");
                }
                finalPayload = offloadResult.Value;
                isOffloadedReference = true;
            }
            else if (resolvedStrategy == LargePayloadStrategy.Segmenting 
                     && byteCount > topicConfig.MaxSegmentSizeBytes)
            {
                var chunks = ChunkStringByBytes(rawPayloadJson, topicConfig.MaxSegmentSizeBytes);
                var assemblyKey = Guid.NewGuid().ToString("N");
                var assemblyCorrelationId = Activity.Current?.RootId ?? Guid.NewGuid().ToString();

                for (var segmentIndex = 0; segmentIndex < chunks.Count; segmentIndex++)
                {
                    var chunkPayload = chunks[segmentIndex];
                    var segmentMessageId = Guid.NewGuid().ToString();

                    var segmentHeaders = new Dictionary<string, string>
                    {
                        [CustomRequestHeaders.MessageId] = segmentMessageId,
                        [CustomRequestHeaders.CorrelationId] = assemblyCorrelationId,
                        [CustomRequestHeaders.EventSchemaVersion] = typeof(TMessage).Name + ".v" + MessageVersionResolver.GetMessageVersion(message),
                        [CustomRequestHeaders.AssemblyKey] = assemblyKey,
                        [CustomRequestHeaders.SegmentIndex] = segmentIndex.ToString(),
                        [CustomRequestHeaders.TotalSegments] = chunks.Count.ToString()
                    };

                    if (activity is not null)
                    {
                        segmentHeaders["traceparent"] = activity.Id ?? string.Empty;
                    }

                    if (topicConfig.EnableAuthentication)
                    {
                        var signature = _signatureService.SignMessage(
                            chunkPayload,
                            segmentMessageId,
                            assemblyCorrelationId,
                            topicConfig.TopicName,
                            messageKey,
                            segmentHeaders[CustomRequestHeaders.EventSchemaVersion],
                            KafkaEnvelopeKinds.Inline);
                        segmentHeaders[CustomRequestHeaders.MessageSignature] = signature;
                    }

                    var segmentClusterAlias = string.IsNullOrWhiteSpace(topicConfig.Cluster) ? _options.DefaultClusterAlias : topicConfig.Cluster;
                    var segmentPublishResult = await _producerManager.PublishDirectAsync(
                        topicConfig.TopicName,
                        segmentClusterAlias,
                        messageKey,
                        chunkPayload,
                        segmentHeaders,
                        topicConfig.Username,
                        topicConfig.Password,
                        cancellationToken);

                    if (!segmentPublishResult.IsSuccess)
                    {
                        return Result.Failure($"Segment publish failed at index {segmentIndex}.");
                    }
                }

                _metricsCollector?.RecordPublish(topicConfig.TopicName, byteCount);
                return Result.Success();
            }

            // 2. Normal (or Offloaded) single message publishing
            var messageId = Guid.NewGuid().ToString();
            var correlationId = Activity.Current?.RootId ?? Guid.NewGuid().ToString();

            var headers = new Dictionary<string, string>
            {
                [CustomRequestHeaders.MessageId] = messageId,
                [CustomRequestHeaders.CorrelationId] = correlationId,
                [CustomRequestHeaders.EventSchemaVersion] = typeof(TMessage).Name + ".v" + MessageVersionResolver.GetMessageVersion(message)
            };

            // Inject trace contexts if present
            if (activity is not null)
            {
                headers["traceparent"] = activity.Id ?? string.Empty;
            }

            // 3. Cryptographic Signature
            if (topicConfig.EnableAuthentication)
            {
                var signature = _signatureService.SignMessage(
                    finalPayload,
                    messageId,
                    correlationId,
                    topicConfig.TopicName,
                    messageKey,
                    headers[CustomRequestHeaders.EventSchemaVersion],
                    isOffloadedReference ? KafkaEnvelopeKinds.S3Reference : KafkaEnvelopeKinds.Inline);
                headers[CustomRequestHeaders.MessageSignature] = signature;
            }

            // 4. Broker Publish
            var clusterAlias = string.IsNullOrWhiteSpace(topicConfig.Cluster) ? _options.DefaultClusterAlias : topicConfig.Cluster;

            var publishResult = await _producerManager.PublishDirectAsync(
                topicConfig.TopicName,
                clusterAlias,
                messageKey,
                finalPayload,
                headers,
                topicConfig.Username,
                topicConfig.Password,
                cancellationToken);

            if (publishResult.IsSuccess)
            {
                _metricsCollector?.RecordPublish(topicConfig.TopicName, Encoding.UTF8.GetByteCount(finalPayload));
                return Result.Success();
            }

            return Result.Failure("Kafka broker publish was rejected.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError("Kafka publish failed for message type {MessageType}. ErrorType: {ErrorType}.",
                typeof(TMessage).FullName, exception.GetType().Name);
            return Result.Failure($"Kafka publish failed with error type '{exception.GetType().Name}'.");
        }
    }

    #region Subscription entry points

    /// <summary>
    /// Resolves and validates all subscription prerequisites (config key, topic config, cluster, consumer group)
    /// shared by both handler-based and channel-based subscription modes.
    /// </summary>
    /// <returns>
    /// A tuple of (topicConfig, clusterConfig, consumerGroup) on success, or a failure Result.
    /// </returns>
    private Result<(KafkaTopicConfiguration TopicConfig, KafkaClusterConfiguration ClusterConfig, string ConsumerGroup)>
        ResolveSubscription<TMessage>(KafkaSubscriptionOptions subscriptionOptions)
    {
        var configurationKey = ResolveConfigurationKey<TMessage>();
        if (!_options.Topics.TryGetValue(configurationKey, out var topicConfig))
        {
            return Result.Failure<(KafkaTopicConfiguration, KafkaClusterConfiguration, string)>(
                $"Kafka configuration key '{configurationKey}' is not defined in options.");
        }

        // Enforce read permissions
        if (topicConfig.Permission != TopicPermission.Read && topicConfig.Permission != TopicPermission.ReadWrite)
        {
            throw new SecurityException($"Unauthorized subscribe attempt. Read access is restricted on topic configuration '{configurationKey}'.");
        }

        var clusterAlias = string.IsNullOrWhiteSpace(topicConfig.Cluster) ? _options.DefaultClusterAlias : topicConfig.Cluster;
        KafkaClusterConfiguration clusterConfig;
        if (!_options.Clusters.TryGetValue(clusterAlias, out clusterConfig!))
        {
            if (!string.IsNullOrWhiteSpace(_options.Servers))
            {
                clusterConfig = new KafkaClusterConfiguration { BootstrapServers = _options.Servers };
            }
            else
            {
                return Result.Failure<(KafkaTopicConfiguration, KafkaClusterConfiguration, string)>(
                    $"Kafka cluster configuration for '{clusterAlias}' is not defined and no root 'Servers' fallback is configured.");
            }
        }

        // Resolve consumer group: subscription override -> topic config -> error
        var consumerGroup = !string.IsNullOrWhiteSpace(subscriptionOptions.ConsumerGroup)
            ? subscriptionOptions.ConsumerGroup
            : topicConfig.GroupId;

        if (string.IsNullOrWhiteSpace(consumerGroup))
        {
            return Result.Failure<(KafkaTopicConfiguration, KafkaClusterConfiguration, string)>(
                $"Consumer group must be configured for subscriptions on '{topicConfig.TopicName}'.");
        }

        if (subscriptionOptions.ReadMode == KafkaReadMode.HistoricalReplay)
        {
            if (string.IsNullOrWhiteSpace(subscriptionOptions.ConsumerGroup))
            {
                return Result.Failure<(KafkaTopicConfiguration, KafkaClusterConfiguration, string)>(
                    $"Historical replay for '{topicConfig.TopicName}' requires an explicit isolated ConsumerGroup override.");
            }

            if (!string.IsNullOrWhiteSpace(topicConfig.GroupId)
                && string.Equals(subscriptionOptions.ConsumerGroup, topicConfig.GroupId, StringComparison.Ordinal))
            {
                return Result.Failure<(KafkaTopicConfiguration, KafkaClusterConfiguration, string)>(
                    $"Historical replay for '{topicConfig.TopicName}' cannot use the live consumer group '{topicConfig.GroupId}'.");
            }

            if (!TryParseHistoricalReplayStart(subscriptionOptions.StartOffsetTime, out _, out _))
            {
                return Result.Failure<(KafkaTopicConfiguration, KafkaClusterConfiguration, string)>(
                    $"Historical replay for '{topicConfig.TopicName}' requires a non-negative offset or an ISO 8601 timestamp.");
            }
        }

        return Result.Success((topicConfig, clusterConfig, consumerGroup));
    }

    /// <summary>
    /// Subscribes to messages on their configured topic with a handler for automatic processing.
    /// </summary>
    /// <typeparam name="TMessage">The type of the message.</typeparam>
    /// <typeparam name="THandler">The message handler type.</typeparam>
    /// <param name="subscriptionOptions">Configuration options for this subscription.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A railway-oriented Result indicating subscription success.</returns>
    internal async Task<Result> SubscribeWithHandlerAsync<TMessage, THandler>(
        KafkaSubscriptionOptions subscriptionOptions,
        CancellationToken cancellationToken = default)
        where TMessage : class
        where THandler : IMessageHandler<TMessage>
    {
        if (subscriptionOptions is null) throw new ArgumentNullException(nameof(subscriptionOptions));
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        var resolution = ResolveSubscription<TMessage>(subscriptionOptions);
        if (!resolution.IsSuccess)
        {
            return Result.Failure(resolution.Error);
        }

        var (topicConfig, clusterConfig, consumerGroup) = resolution.Value;
        var consumerKey = new ConsumerTaskKey(topicConfig.TopicName, consumerGroup);

        var startGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var startupSignal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumerCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            _shutdownCancellation.Token);
        var task = Task.Run(async () =>
        {
            await startGate.Task.ConfigureAwait(false);
            await SupervisedConsumerLoopAsync<TMessage>(
                topicConfig, clusterConfig, subscriptionOptions, consumerGroup,
                "handler",
                collectRawHeaders: false,
                async (consumeResult, extractedHeaders, consumer, token) =>
                    await ProcessHandlerMessageAsync<TMessage, THandler>(
                        consumeResult, extractedHeaders, topicConfig, consumerGroup, consumer, token),
                onShutdown: null,
                startupSignal,
                consumerCancellation.Token);
        }, CancellationToken.None);
        _ = task.ContinueWith(_ => consumerCancellation.Dispose(), TaskScheduler.Default);

        if (!_consumerTasks.TryAdd(consumerKey, task))
        {
            startGate.TrySetCanceled(cancellationToken);
            return Result.Failure($"A Kafka handler subscription for topic '{topicConfig.TopicName}' and group '{consumerGroup}' is already running.");
        }

        if (Volatile.Read(ref _disposed) != 0)
        {
            CancelPendingConsumerStart(consumerCancellation, startGate);
            TryRemoveConsumerTask(consumerKey, task);
            return Result.Failure("Kafka message bus is shutting down.");
        }

        ObserveTaskCompletion(task, consumerKey, topicConfig.TopicName, consumerGroup, "handler");
        startGate.TrySetResult(true);

        try
        {
            await startupSignal.Task.WaitAsync(
                TimeSpan.FromMilliseconds(_options.Consumer.StartupTimeoutMs),
                cancellationToken).ConfigureAwait(false);
            return Result.Success();
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            consumerCancellation.Cancel();
            TryRemoveConsumerTask(consumerKey, task);
            return Result.Failure($"Kafka handler subscription startup failed. ErrorType: {exception.GetType().Name}.");
        }
    }

    /// <summary>
    /// Subscribes to messages on their configured topic in handler-less consumer mode.
    /// Messages are written to the <see cref="KafkaConsumer{TMessage}"/> channel for manual consumption.
    /// </summary>
    /// <typeparam name="TMessage">The type of the message.</typeparam>
    /// <param name="subscriptionOptions">Configuration options for this subscription.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A railway-oriented Result indicating subscription success.</returns>
    internal async Task<Result> SubscribeConsumerAsync<TMessage>(
        KafkaSubscriptionOptions subscriptionOptions,
        CancellationToken cancellationToken = default)
        where TMessage : class
    {
        if (subscriptionOptions is null) throw new ArgumentNullException(nameof(subscriptionOptions));
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        var resolution = ResolveSubscription<TMessage>(subscriptionOptions);
        if (!resolution.IsSuccess)
        {
            return Result.Failure(resolution.Error);
        }

        var (topicConfig, clusterConfig, consumerGroup) = resolution.Value;
        var consumerKey = new ConsumerTaskKey(topicConfig.TopicName, consumerGroup);

        var startGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var startupSignal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumerCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            _shutdownCancellation.Token);
        var task = Task.Run(async () =>
        {
            await startGate.Task.ConfigureAwait(false);
            await SupervisedConsumerLoopAsync<TMessage>(
                topicConfig, clusterConfig, subscriptionOptions, consumerGroup,
                "channel",
                collectRawHeaders: true,
                async (consumeResult, extractedHeaders, consumer, token) =>
                    await ProcessChannelMessageAsync<TMessage>(
                        consumeResult, extractedHeaders, topicConfig, consumerGroup, consumer, token),
                onShutdown: () =>
                {
                    var kafkaConsumer = _serviceProvider.GetService<KafkaConsumer<TMessage>>();
                    kafkaConsumer?.Complete();
                },
                startupSignal,
                consumerCancellation.Token);
        }, CancellationToken.None);
        _ = task.ContinueWith(_ => consumerCancellation.Dispose(), TaskScheduler.Default);

        if (!_consumerTasks.TryAdd(consumerKey, task))
        {
            startGate.TrySetCanceled(cancellationToken);
            return Result.Failure($"A Kafka channel subscription for topic '{topicConfig.TopicName}' and group '{consumerGroup}' is already running.");
        }

        if (Volatile.Read(ref _disposed) != 0)
        {
            CancelPendingConsumerStart(consumerCancellation, startGate);
            TryRemoveConsumerTask(consumerKey, task);
            return Result.Failure("Kafka message bus is shutting down.");
        }

        ObserveTaskCompletion(task, consumerKey, topicConfig.TopicName, consumerGroup, "channel");
        startGate.TrySetResult(true);

        try
        {
            await startupSignal.Task.WaitAsync(
                TimeSpan.FromMilliseconds(_options.Consumer.StartupTimeoutMs),
                cancellationToken).ConfigureAwait(false);
            return Result.Success();
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            consumerCancellation.Cancel();
            TryRemoveConsumerTask(consumerKey, task);
            return Result.Failure($"Kafka channel subscription startup failed. ErrorType: {exception.GetType().Name}.");
        }
    }

    /// <summary>
    /// Observes task completion to prevent unobserved exceptions and logs critical failures.
    /// </summary>
    private void ObserveTaskCompletion(Task task, ConsumerTaskKey consumerKey, string topicName, string consumerGroup, string mode)
    {
        _ = task.ContinueWith(completedTask =>
        {
            TryRemoveConsumerTask(consumerKey, task);
            if (completedTask.IsFaulted)
            {
                _logger.LogCritical(
                    "Consumer ({Mode}) for topic '{TopicName}' in group '{ConsumerGroup}' terminated with an unhandled exception. ErrorType: {ErrorType}.",
                    mode, topicName, consumerGroup, completedTask.Exception?.GetBaseException().GetType().Name ?? "Unknown");
            }
        }, TaskScheduler.Default);
    }

    private void TryRemoveConsumerTask(ConsumerTaskKey consumerKey, Task task)
    {
        ((ICollection<KeyValuePair<ConsumerTaskKey, Task>>)_consumerTasks)
            .Remove(new KeyValuePair<ConsumerTaskKey, Task>(consumerKey, task));
    }

    internal static void CancelPendingConsumerStart(
        CancellationTokenSource consumerCancellation,
        TaskCompletionSource<bool> startGate)
    {
        consumerCancellation.Cancel();
        startGate.TrySetResult(true);
    }

    internal static bool IsFatalConsumeFailure(Exception exception)
    {
        return exception is ConsumeException { Error.IsFatal: true };
    }

    internal static int CalculateBoundedExponentialDelayMs(int baseDelayMs, int maxDelayMs, int exponent)
    {
        if (baseDelayMs <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(baseDelayMs));
        }

        if (maxDelayMs < baseDelayMs)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDelayMs));
        }

        var delay = baseDelayMs;
        for (var index = 0; index < exponent && delay < maxDelayMs; index++)
        {
            delay = (int)Math.Min((long)maxDelayMs, (long)delay * 2);
        }

        return delay;
    }

    #endregion

    #region Unified consumer loop

    /// <summary>
    /// Unified supervised consumer loop that handles both handler-based and channel-based subscriptions.
    /// Wraps the inner consumer loop in a supervision envelope with automatic restart and exponential backoff.
    /// </summary>
    /// <param name="topicConfig">The topic configuration.</param>
    /// <param name="clusterConfig">The cluster configuration.</param>
    /// <param name="subscriptionOptions">Subscription options.</param>
    /// <param name="consumerGroup">The consumer group identifier.</param>
    /// <param name="mode">A label for logging (e.g., "handler" or "channel").</param>
    /// <param name="collectRawHeaders">When true, all Kafka headers are collected into a dictionary for channel-mode consumers.</param>
    /// <param name="messageProcessor">The delegate that processes each consumed message.</param>
    /// <param name="onShutdown">Optional action to perform on graceful shutdown (e.g., signal channel completion).</param>
    /// <param name="startupSignal">Signals successful consumer construction or propagates the initial startup failure.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task SupervisedConsumerLoopAsync<TMessage>(
        KafkaTopicConfiguration topicConfig,
        KafkaClusterConfiguration clusterConfig,
        KafkaSubscriptionOptions subscriptionOptions,
        string consumerGroup,
        string mode,
        bool collectRawHeaders,
        Func<ConsumeResult<string, string>, ConsumedMessageHeaders, IConsumer<string, string>, CancellationToken, Task<bool>> messageProcessor,
        Action? onShutdown,
        TaskCompletionSource<bool> startupSignal,
        CancellationToken cancellationToken)
        where TMessage : class
    {
        var restartCount = 0;
        var maxRestarts = _options.Consumer.MaxRestartAttempts;
        var baseDelay = _options.Consumer.RestartBaseDelayMs;
        var maxDelay = _options.Consumer.MaxRestartDelayMs;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await RunConsumerLoopAsync<TMessage>(
                    topicConfig, clusterConfig, subscriptionOptions, consumerGroup,
                    mode, collectRawHeaders, messageProcessor, startupSignal, cancellationToken);

                _logger.LogInformation("Consumer ({Mode}) for topic '{TopicName}' in group '{ConsumerGroup}' exited gracefully.",
                    mode, topicConfig.TopicName, consumerGroup);
                break;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Consumer ({Mode}) for topic '{TopicName}' in group '{ConsumerGroup}' shutdown via cancellation.",
                    mode, topicConfig.TopicName, consumerGroup);
                break;
            }
            catch (Exception exception)
            {
                if (startupSignal.TrySetException(exception))
                {
                    throw;
                }

                restartCount++;
                var maxRestartsDisplay = maxRestarts == 0 ? "unlimited" : maxRestarts.ToString();

                _logger.LogError(
                    "Consumer ({Mode}) for topic '{TopicName}' in group '{ConsumerGroup}' crashed (restart {RestartCount}/{MaxRestarts}). ErrorType: {ErrorType}.",
                    mode, topicConfig.TopicName, consumerGroup, restartCount, maxRestartsDisplay, exception.GetType().Name);

                if (maxRestarts > 0 && restartCount >= maxRestarts)
                {
                    _logger.LogCritical(
                        "Consumer ({Mode}) for topic '{TopicName}' in group '{ConsumerGroup}' exceeded maximum restart attempts ({MaxRestarts}). Consumer is permanently stopped.",
                        mode, topicConfig.TopicName, consumerGroup, maxRestarts);
                    throw;
                }

                var delay = CalculateBoundedExponentialDelayMs(baseDelay, maxDelay, restartCount - 1);
                _logger.LogWarning("Restarting consumer ({Mode}) for topic '{TopicName}' in {DelayMs}ms.",
                    mode, topicConfig.TopicName, delay);
                await Task.Delay(delay, cancellationToken);
            }
        }

        onShutdown?.Invoke();
    }

    /// <summary>
    /// The inner consumer loop that polls Kafka and delegates each message to the provided processor.
    /// </summary>
    private async Task RunConsumerLoopAsync<TMessage>(
        KafkaTopicConfiguration topicConfig,
        KafkaClusterConfiguration clusterConfig,
        KafkaSubscriptionOptions subscriptionOptions,
        string consumerGroup,
        string mode,
        bool collectRawHeaders,
        Func<ConsumeResult<string, string>, ConsumedMessageHeaders, IConsumer<string, string>, CancellationToken, Task<bool>> messageProcessor,
        TaskCompletionSource<bool> startupSignal,
        CancellationToken cancellationToken)
        where TMessage : class
    {
        var config = BuildConsumerConfig(topicConfig, clusterConfig, subscriptionOptions, consumerGroup);
        var builder = new ConsumerBuilder<string, string>(config);

        builder.SetErrorHandler((_, error) =>
        {
            if (error.IsFatal)
            {
                startupSignal.TrySetException(new KafkaException(error));
            }
        });
        ConfigureHistoricalReplay(builder, subscriptionOptions, topicConfig, startupSignal);

        using var consumer = builder.Build();
        consumer.Subscribe(topicConfig.TopicName);

        _logger.LogInformation("Successfully subscribed to Kafka topic '{TopicName}' using group '{ConsumerGroup}' in {Mode} mode.",
            topicConfig.TopicName, consumerGroup, mode);

        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumeResult<string, string> consumeResult;
            try
            {
                consumeResult = consumer.Consume(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception exception) when (IsFatalConsumeFailure(exception))
            {
                throw;
            }
            catch (Exception exception)
            {
                if (!startupSignal.Task.IsCompleted)
                {
                    throw;
                }

                _logger.LogError("Error consuming messages from Kafka topic '{TopicName}' ({Mode}). ErrorType: {ErrorType}.",
                    topicConfig.TopicName, mode, exception.GetType().Name);
                await Task.Delay(_options.Consumer.ConsumeErrorDelayMs, cancellationToken);
                continue;
            }

            if (consumeResult is null || consumeResult.IsPartitionEOF)
            {
                continue;
            }

            var stopwatch = Stopwatch.StartNew();
            ConsumedMessageHeaders extractedHeaders;
            try
            {
                extractedHeaders = ConsumedMessageHeaders.Extract<TMessage>(
                    consumeResult,
                    collectRawHeaders,
                    allowGeneratedMessageIdFallback: !topicConfig.IsIdempotent);
            }
            catch (Exception exception) when (exception is InvalidDataException or OverflowException)
            {
                _logger.LogWarning(
                    "Rejected Kafka record with invalid transport headers on topic '{TopicName}', partition {Partition}, offset {Offset}.",
                    topicConfig.TopicName,
                    consumeResult.Partition.Value,
                    consumeResult.Offset.Value);

                var dlqConfig = BuildDlqConfig(topicConfig);
                if (!dlqConfig.IsEnabled)
                {
                    throw;
                }

                var fallbackHeaders = new ConsumedMessageHeaders
                {
                    MessageId = Guid.NewGuid().ToString(),
                    HasValidMessageIdHeader = false,
                    CorrelationId = Guid.NewGuid().ToString(),
                    SchemaVersionKey = typeof(TMessage).Name + ".v" + MessageVersionResolver.GetMessageVersion<TMessage>(),
                    RawHeaders = new Dictionary<string, string>()
                };
                var clusterAlias = string.IsNullOrWhiteSpace(topicConfig.Cluster)
                    ? _options.DefaultClusterAlias
                    : topicConfig.Cluster;
                var routingResult = await RouteToDlqAsync(
                    topicConfig.TopicName,
                    dlqConfig.TargetDlqTopic,
                    clusterAlias,
                    consumeResult,
                    exception,
                    fallbackHeaders,
                    topicConfig,
                    cancellationToken);
                if (!routingResult.IsSuccess)
                {
                    throw new InvalidOperationException("Invalid-header record could not be routed to DLQ.");
                }

                consumer.Commit(consumeResult);
                _metricsCollector?.RecordConsume(topicConfig.TopicName, false, stopwatch.ElapsedMilliseconds);
                continue;
            }

            using var logScope = _logger.BeginScope(new Dictionary<string, object>
            {
                ["Topic"] = topicConfig.TopicName,
                ["ConsumerGroup"] = consumerGroup
            });

            using var activity = ActivitySource.StartActivity(
                "CSharpExtensions.Kafka.consume",
                ActivityKind.Consumer,
                extractedHeaders.Traceparent ?? string.Empty);

            activity?.SetTag("messaging.system", "kafka");
            activity?.SetTag("messaging.destination", topicConfig.TopicName);

            var isSuccess = await messageProcessor(consumeResult, extractedHeaders, consumer, cancellationToken);

            stopwatch.Stop();
            _metricsCollector?.RecordConsume(topicConfig.TopicName, isSuccess, stopwatch.ElapsedMilliseconds);
        }

        try
        {
            consumer.Close();
        }
        catch
        {
            // Suppress close exceptions on cancellation shutdown
        }
    }

    #endregion

    #region Message processors (handler vs channel)

    /// <summary>
    /// Processes a single message in handler mode: runs the ROP pipeline, invokes the handler with retries,
    /// commits offset on success, routes to DLQ on failure.
    /// </summary>
    /// <returns>True if the message was successfully processed or routed.</returns>
    private async Task<bool> ProcessHandlerMessageAsync<TMessage, THandler>(
        ConsumeResult<string, string> consumeResult,
        ConsumedMessageHeaders extractedHeaders,
        KafkaTopicConfiguration topicConfig,
        string consumerGroup,
        IConsumer<string, string> consumer,
        CancellationToken cancellationToken)
        where TMessage : class
        where THandler : IMessageHandler<TMessage>
    {
        EnsureValidIdempotencyMessageId(extractedHeaders, topicConfig);

        var rawValue = consumeResult.Message.Value;
        var ownerToken = topicConfig.IsIdempotent ? Guid.NewGuid().ToString() : null;

        // Execute the ROP pipeline
        var pipelineResult = await ValidateInboundPayload(rawValue)
            .Then(payload => VerifySignature(
                payload,
                extractedHeaders.MessageId,
                extractedHeaders.CorrelationId,
                extractedHeaders.SchemaVersionKey,
                extractedHeaders.Signature,
                consumeResult.Message.Key,
                topicConfig))
            .ThenAsync(payload => DownloadOffloadIfNeededAsync(payload, topicConfig, cancellationToken))
            .ThenAsync(payload => ClaimUniqueIfNeededAsync(payload, extractedHeaders.MessageId, consumerGroup, topicConfig, ownerToken, cancellationToken))
            .ThenAsync(payload => ResolveAndUpcastMessage<TMessage>(
                payload,
                extractedHeaders.SchemaVersionKey))
            .ThenAsync(payload => DeserializeMessage<TMessage>(payload))
            .ThenAsync(messageInstance => VerifyMessageVersion(messageInstance, topicConfig))
            .ThenAsync(messageInstance => ExecuteHandlerWithClaimRenewalAsync<TMessage, THandler>(
                messageInstance,
                extractedHeaders.MessageId,
                consumerGroup,
                ownerToken,
                topicConfig,
                cancellationToken));

        if (pipelineResult.IsSuccess)
        {
            if (topicConfig.IsIdempotent)
            {
                var completeResult = await _duplicateDetector.CompleteClaimAsync(
                    extractedHeaders.MessageId,
                    consumerGroup,
                    cancellationToken,
                    topicConfig.IdempotencyRetentionSeconds,
                    ownerToken);

                if (!completeResult.IsSuccess)
                {
                    _logger.LogCritical("Failed to complete an idempotency claim. Halting consumer to prevent double processing.");
                    throw new InvalidOperationException("Critical failure completing idempotency claim in Redis.");
                }
            }

            consumer.Commit(consumeResult);
            return true;
        }

        return await HandlePipelineFailureAsync(
            pipelineResult, consumeResult, extractedHeaders, topicConfig, consumerGroup, consumer, ownerToken, cancellationToken);
    }

    /// <summary>
    /// Processes a single message in channel mode: runs the ROP pipeline, writes to the KafkaConsumer channel,
    /// waits for acknowledge/reject from user code.
    /// </summary>
    /// <returns>True if the message was successfully processed or routed.</returns>
    private async Task<bool> ProcessChannelMessageAsync<TMessage>(
        ConsumeResult<string, string> consumeResult,
        ConsumedMessageHeaders extractedHeaders,
        KafkaTopicConfiguration topicConfig,
        string consumerGroup,
        IConsumer<string, string> consumer,
        CancellationToken cancellationToken)
        where TMessage : class
    {
        EnsureValidIdempotencyMessageId(extractedHeaders, topicConfig);

        var rawValue = consumeResult.Message.Value;
        var ownerToken = topicConfig.IsIdempotent ? Guid.NewGuid().ToString() : null;

        // Execute the ROP pipeline (signature -> offload -> dedup -> upcast -> deserialize)
        var pipelineResult = await ValidateInboundPayload(rawValue)
            .Then(payload => VerifySignature(
                payload,
                extractedHeaders.MessageId,
                extractedHeaders.CorrelationId,
                extractedHeaders.SchemaVersionKey,
                extractedHeaders.Signature,
                consumeResult.Message.Key,
                topicConfig))
            .ThenAsync(payload => DownloadOffloadIfNeededAsync(payload, topicConfig, cancellationToken))
            .ThenAsync(payload => ClaimUniqueIfNeededAsync(payload, extractedHeaders.MessageId, consumerGroup, topicConfig, ownerToken, cancellationToken))
            .ThenAsync(payload => ResolveAndUpcastMessage<TMessage>(
                payload,
                extractedHeaders.SchemaVersionKey))
            .ThenAsync(payload => DeserializeMessage<TMessage>(payload))
            .ThenAsync(messageInstance => VerifyMessageVersion(messageInstance, topicConfig));

        if (pipelineResult.IsSuccess)
        {
            return await DispatchToChannelAsync(
                pipelineResult.Value, consumeResult, extractedHeaders, topicConfig, consumerGroup, consumer, ownerToken, cancellationToken);
        }

        return await HandlePipelineFailureAsync(
            pipelineResult, consumeResult, extractedHeaders, topicConfig, consumerGroup, consumer, ownerToken, cancellationToken);
    }

    /// <summary>
    /// Dispatches a deserialized message to the KafkaConsumer channel and waits for acknowledge/reject.
    /// </summary>
    private async Task<bool> DispatchToChannelAsync<TMessage>(
        TMessage messageInstance,
        ConsumeResult<string, string> consumeResult,
        ConsumedMessageHeaders extractedHeaders,
        KafkaTopicConfiguration topicConfig,
        string consumerGroup,
        IConsumer<string, string> consumer,
        string? ownerToken,
        CancellationToken cancellationToken)
        where TMessage : class
    {
        var kafkaConsumer = _serviceProvider.GetRequiredService<KafkaConsumer<TMessage>>();

        // Create a TaskCompletionSource to wait for user acknowledge/reject
        var completionSource = new TaskCompletionSource<(bool Acknowledged, string? RejectReason)>(TaskCreationOptions.RunContinuationsAsynchronously);

        var context = new ConsumeContext<TMessage>(
            message: messageInstance,
            messageId: extractedHeaders.MessageId,
            correlationId: extractedHeaders.CorrelationId,
            topic: consumeResult.Topic,
            partition: consumeResult.Partition.Value,
            offset: consumeResult.Offset.Value,
            timestamp: consumeResult.Message.Timestamp.UtcDateTime,
            headers: extractedHeaders.RawHeaders,
            acknowledgeAction: () =>
            {
                completionSource.TrySetResult((true, null));
                return Task.CompletedTask;
            },
            rejectAction: reason =>
            {
                completionSource.TrySetResult((false, reason));
                return Task.CompletedTask;
            });

        // Write to the channel for the user to consume
        await kafkaConsumer.WriteAsync(context, cancellationToken);

        // Wait for the user code to acknowledge or reject while preserving the owner-fenced claim.
        var (acknowledged, _) = await WaitWithClaimRenewalAsync(
            completionSource.Task,
            extractedHeaders.MessageId,
            consumerGroup,
            ownerToken,
            topicConfig,
            cancellationToken);

        if (acknowledged)
        {
            if (topicConfig.IsIdempotent)
            {
                var completeResult = await _duplicateDetector.CompleteClaimAsync(
                    extractedHeaders.MessageId,
                    consumerGroup,
                    cancellationToken,
                    topicConfig.IdempotencyRetentionSeconds,
                    ownerToken);

                if (!completeResult.IsSuccess)
                {
                    _logger.LogCritical("Failed to complete an idempotency claim in channel mode. Halting consumer to prevent double processing.");
                    throw new InvalidOperationException("Critical failure completing idempotency claim in Redis.");
                }
            }

            consumer.Commit(consumeResult);
            return true;
        }

        _logger.LogWarning("A message was rejected by the channel consumer.");

        if (topicConfig.IsIdempotent && !string.IsNullOrEmpty(ownerToken))
        {
            await _duplicateDetector.ReleaseClaimAsync(extractedHeaders.MessageId, consumerGroup, cancellationToken, ownerToken);
        }

        var dlqConfig = BuildDlqConfig(topicConfig);

        if (dlqConfig.IsEnabled)
        {
            var exception = new InvalidOperationException("ChannelConsumerRejected");
            var clusterAlias = string.IsNullOrWhiteSpace(topicConfig.Cluster) ? _options.DefaultClusterAlias : topicConfig.Cluster;

            var routingResult = await RouteToDlqAsync(
                topicConfig.TopicName, dlqConfig.TargetDlqTopic, clusterAlias,
                consumeResult, exception, extractedHeaders, topicConfig, cancellationToken);

            if (routingResult.IsSuccess)
            {
                _logger.LogInformation("Successfully routed a rejected message to DLQ topic '{DlqTopic}'.", dlqConfig.TargetDlqTopic);
                consumer.Commit(consumeResult);
                return true;
            }

            _logger.LogCritical("Failed routing a rejected message to Dead Letter Queue. Halting consumption to prevent data loss.");
            throw new InvalidOperationException("Critical failure routing message to DLQ. Halting consumer loop to prevent data loss.");
        }

        // DLQ disabled - commit offset anyway since user explicitly rejected
        consumer.Commit(consumeResult);
        return true;
    }

    #endregion

    #region Shared pipeline and DLQ infrastructure

    /// <summary>
    /// Handles a pipeline failure result, routes to DLQ if enabled, and throws if DLQ routing fails.
    /// Shared by both handler and channel consumer modes.
    /// </summary>
    /// <returns>True if the failure was handled (e.g., duplicate skip, DLQ routed). Throws on critical failure.</returns>
    private async Task<bool> HandlePipelineFailureAsync(
        Result pipelineResult,
        ConsumeResult<string, string> consumeResult,
        ConsumedMessageHeaders extractedHeaders,
        KafkaTopicConfiguration topicConfig,
        string consumerGroup,
        IConsumer<string, string> consumer,
        string? ownerToken,
        CancellationToken cancellationToken)
    {
        if (string.Equals(
            pipelineResult.Error.Type,
            RedisDistributedDuplicateDetector.InFlightErrorType,
            StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "Message processing is already claimed by another consumer. The offset was not committed.");
        }

        if (string.Equals(pipelineResult.Error.Type, "Conflict", StringComparison.OrdinalIgnoreCase))
        {
            // Confirmed duplicate message. Commit offset and skip handler processing.
            consumer.Commit(consumeResult);
            return true;
        }

        // Release the Processing claim because the handler or validation failed.
        if (topicConfig.IsIdempotent && !string.IsNullOrEmpty(ownerToken))
        {
            await _duplicateDetector.ReleaseClaimAsync(extractedHeaders.MessageId, consumerGroup, cancellationToken, ownerToken);
        }

        _logger.LogError("Message processing pipeline failed. ErrorType: {ErrorType}.", pipelineResult.Error.Type);

        var dlqConfig = BuildDlqConfig(topicConfig);

        if (dlqConfig.IsEnabled)
        {
            var exception = new InvalidOperationException("KafkaPipelineRejected");

            var clusterAlias = string.IsNullOrWhiteSpace(topicConfig.Cluster) ? _options.DefaultClusterAlias : topicConfig.Cluster;

            var routingResult = await RouteToDlqAsync(
                topicConfig.TopicName, dlqConfig.TargetDlqTopic, clusterAlias,
                consumeResult, exception, extractedHeaders, topicConfig, cancellationToken);

            if (routingResult.IsSuccess)
            {
                _logger.LogInformation("Successfully routed a failed message to DLQ topic '{DlqTopic}'.", dlqConfig.TargetDlqTopic);
                consumer.Commit(consumeResult);
                return true;
            }

            _logger.LogCritical("Failed routing a failed message to Dead Letter Queue. Halting consumption to prevent data loss.");
            throw new InvalidOperationException("Critical failure routing message to DLQ. Halting consumer loop to prevent data loss.");
        }

        _logger.LogCritical(
            "DLQ is disabled. Halting consumption to prevent data loss. ErrorType: {ErrorType}.",
            pipelineResult.Error.Type);
        throw new InvalidOperationException("Consumer pipeline failed and DLQ is disabled. Halting consumer loop to prevent data loss.");
    }

    /// <summary>
    /// Builds a <see cref="DlqConfigurationOptions"/> from the topic configuration.
    /// </summary>
    private static DlqConfigurationOptions BuildDlqConfig(KafkaTopicConfiguration topicConfig) =>
        new(
            IsEnabled: topicConfig.EnableDlq,
            TargetDlqTopic: string.IsNullOrWhiteSpace(topicConfig.TargetDlqTopic)
                ? $"{topicConfig.TopicName}.dlq"
                : topicConfig.TargetDlqTopic
        );

    /// <summary>
    /// Builds the Confluent.Kafka ConsumerConfig from topic, cluster, and subscription configuration.
    /// </summary>
    private ConsumerConfig BuildConsumerConfig(
        KafkaTopicConfiguration topicConfig,
        KafkaClusterConfiguration clusterConfig,
        KafkaSubscriptionOptions subscriptionOptions,
        string consumerGroup)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = clusterConfig.BootstrapServers,
            AllowAutoCreateTopics = false,
            GroupId = consumerGroup,
            AutoOffsetReset = subscriptionOptions.ReadMode == KafkaReadMode.Latest ? AutoOffsetReset.Latest : AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            SessionTimeoutMs = _options.Consumer.SessionTimeoutMs,
            MaxPollIntervalMs = _options.Consumer.MaxPollIntervalMs,
            FetchMinBytes = _options.Consumer.FetchMinBytes,
            FetchWaitMaxMs = _options.Consumer.FetchMaxWaitMs,
            ReconnectBackoffMs = clusterConfig.ReconnectBackoffMs,
            ReconnectBackoffMaxMs = clusterConfig.ReconnectBackoffMaxMs
        };

        if (!string.IsNullOrWhiteSpace(clusterConfig.SecurityProtocol))
        {
            if (Enum.TryParse<SecurityProtocol>(clusterConfig.SecurityProtocol, true, out var protocol))
            {
                config.SecurityProtocol = protocol;
            }
            else
            {
                throw new ArgumentException($"Invalid SecurityProtocol value '{clusterConfig.SecurityProtocol}' configured for topic '{topicConfig.TopicName}'.");
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
                throw new ArgumentException($"Invalid SaslMechanism value '{clusterConfig.SaslMechanism}' configured for topic '{topicConfig.TopicName}'.");
            }
        }

        // Resolve credentials: topic-level overrides cluster-level
        var effectiveUsername = !string.IsNullOrWhiteSpace(topicConfig.Username) ? topicConfig.Username : clusterConfig.SaslUsername;
        var effectivePassword = !string.IsNullOrWhiteSpace(topicConfig.Password) ? topicConfig.Password : clusterConfig.SaslPassword;

        if (!string.IsNullOrWhiteSpace(effectiveUsername) && !string.IsNullOrWhiteSpace(effectivePassword))
        {
            config.SaslUsername = effectiveUsername;
            config.SaslPassword = effectivePassword;
        }

        return config;
    }

    /// <summary>
    /// Configures the consumer builder for historical replay mode with timestamp-based or offset-based seeking.
    /// </summary>
    private void ConfigureHistoricalReplay(
        ConsumerBuilder<string, string> builder,
        KafkaSubscriptionOptions subscriptionOptions,
        KafkaTopicConfiguration topicConfig,
        TaskCompletionSource<bool> startupSignal)
    {
        if (subscriptionOptions.ReadMode != KafkaReadMode.HistoricalReplay)
        {
            builder.SetPartitionsAssignedHandler((_, _) => startupSignal.TrySetResult(true));
            return;
        }

        if (!TryParseHistoricalReplayStart(subscriptionOptions.StartOffsetTime, out var absoluteOffset, out var timestamp))
        {
            throw new InvalidOperationException(
                $"Historical replay start position for topic '{topicConfig.TopicName}' is invalid.");
        }

        builder.SetPartitionsAssignedHandler((c, partitions) =>
        {
            if (timestamp.HasValue)
            {
                var unixTimeMs = timestamp.Value.ToUnixTimeMilliseconds();
                var timestamps = partitions.Select(p => new TopicPartitionTimestamp(p, new Timestamp(unixTimeMs, TimestampType.CreateTime))).ToList();
                var offsets = c.OffsetsForTimes(timestamps, TimeSpan.FromSeconds(10));
                if (offsets.Any(offset => offset.Offset == Offset.Unset))
                {
                    throw new InvalidOperationException(
                        $"Kafka could not resolve all historical replay offsets for topic '{topicConfig.TopicName}'.");
                }

                foreach (var offset in offsets)
                {
                    c.Seek(offset);
                }
            }
            else
            {
                foreach (var partition in partitions)
                {
                    var watermark = c.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10));
                    if (absoluteOffset!.Value < watermark.Low.Value || absoluteOffset.Value > watermark.High.Value)
                    {
                        throw new InvalidOperationException(
                            $"Historical replay offset is outside retained bounds for topic '{topicConfig.TopicName}', partition {partition.Partition.Value}.");
                    }

                    c.Seek(new TopicPartitionOffset(partition, new Offset(absoluteOffset!.Value)));
                }
            }

            startupSignal.TrySetResult(true);
        });
    }

    internal static bool TryParseHistoricalReplayStart(
        string? value,
        out long? absoluteOffset,
        out DateTimeOffset? timestamp)
    {
        absoluteOffset = null;
        timestamp = null;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        if (long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedOffset))
        {
            if (parsedOffset < 0)
            {
                return false;
            }

            absoluteOffset = parsedOffset;
            return true;
        }

        if (DateTimeOffset.TryParse(
                value,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var parsedTimestamp))
        {
            timestamp = parsedTimestamp;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Verifies the encrypted SHA-256 digest of a consumed message.
    /// Returns the original payload on success or when authentication is disabled.
    /// </summary>
    private Result<string> ValidateInboundPayload(string? payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
        {
            return Result.Failure<string>("Kafka message payload is empty.");
        }

        if (Encoding.UTF8.GetByteCount(payload) > _options.Producer.MaxPayloadBytes)
        {
            return Result.Failure<string>("Kafka message payload exceeds the configured safety limit.");
        }

        return Result.Success(payload);
    }

    private Result<string> VerifySignature(
        string rawValue,
        string messageId,
        string correlationId,
        string schemaVersionKey,
        string? signature,
        string? messageKey,
        KafkaTopicConfiguration topicConfig)
    {
        if (!topicConfig.EnableAuthentication)
        {
            return Result.Success(rawValue);
        }

        if (string.IsNullOrWhiteSpace(signature) || 
            !_signatureService.VerifySignature(
                rawValue,
                messageId,
                correlationId,
                topicConfig.TopicName,
                messageKey,
                schemaVersionKey,
                ResolveEnvelopeKind(rawValue, topicConfig),
                signature))
        {
            return Result.Failure<string>(new RailwayError("Unauthorized Kafka message received because signature verification failed.")
                .AsInternalServer("SecurityViolation", "Signature verification failed."));
        }

        return Result.Success(rawValue);
    }

    internal static string ResolveEnvelopeKind(string payloadJson, KafkaTopicConfiguration topicConfig)
    {
        if (topicConfig.ResolvedStrategy != LargePayloadStrategy.S3Offloading
            || !payloadJson.Contains("\"$ref\"", StringComparison.Ordinal)
            || Encoding.UTF8.GetByteCount(payloadJson) > MaxClaimCheckEnvelopeBytes)
        {
            return KafkaEnvelopeKinds.Inline;
        }

        try
        {
            using var document = JsonDocument.Parse(
                payloadJson,
                new JsonDocumentOptions { MaxDepth = MaxClaimCheckEnvelopeDepth });
            return document.RootElement.TryGetProperty("$ref", out var reference)
                   && reference.ValueKind == JsonValueKind.True
                ? KafkaEnvelopeKinds.S3Reference
                : KafkaEnvelopeKinds.Inline;
        }
        catch (JsonException)
        {
            return KafkaEnvelopeKinds.Inline;
        }
    }

    /// <summary>
    /// Downloads offloaded payload from S3 if the message is a claim check reference.
    /// Uses structured JSON parsing instead of string matching to prevent false positives.
    /// </summary>
    private async Task<Result<string>> DownloadOffloadIfNeededAsync(
        string payloadJson,
        KafkaTopicConfiguration topicConfig,
        CancellationToken cancellationToken)
    {
        if (topicConfig.ResolvedStrategy != LargePayloadStrategy.S3Offloading)
        {
            return Result.Success(payloadJson);
        }

        // Quick pre-filter: skip JSON parsing for messages that definitely are not claim check payloads
        if (!payloadJson.Contains("\"$ref\"", StringComparison.Ordinal))
        {
            return Result.Success(payloadJson);
        }

        if (Encoding.UTF8.GetByteCount(payloadJson) > MaxClaimCheckEnvelopeBytes)
        {
            return Result.Failure<string>(new RailwayError("Kafka claim-check envelope exceeds the permitted size."));
        }

        try
        {
            using var jsonDocument = JsonDocument.Parse(
                payloadJson,
                new JsonDocumentOptions { MaxDepth = MaxClaimCheckEnvelopeDepth });

            // Validate the claim-check marker via structured JSON, not string matching.
            if (!jsonDocument.RootElement.TryGetProperty("$ref", out var refElement) || refElement.ValueKind != JsonValueKind.True)
            {
                return Result.Success(payloadJson);
            }

            var downloadResult = await _offloader.DownloadAsync(jsonDocument.RootElement, _options.Offloading, cancellationToken);
            if (downloadResult.IsSuccess)
            {
                return Result.Success(downloadResult.Value);
            }
            
            return Result.Failure<string>(downloadResult.Error);
        }
        catch (JsonException)
        {
            return Result.Failure<string>(new RailwayError("Kafka claim-check envelope is invalid JSON."));
        }
    }

    /// <summary>
    /// Claims uniqueness for idempotent message processing via distributed duplicate detection.
    /// Returns a Conflict error for duplicate messages, or the original payload on success.
    /// </summary>
    private async Task<Result<string>> ClaimUniqueIfNeededAsync(
        string payloadJson, 
        string messageId, 
        string consumerGroup, 
        KafkaTopicConfiguration topicConfig, 
        string? ownerToken,
        CancellationToken cancellationToken)
    {
        if (topicConfig.IsIdempotent)
        {
            var claimResult = await _duplicateDetector.TryClaimUniqueAsync(
                messageId,
                consumerGroup,
                topicConfig.IdempotencyRetentionSeconds,
                cancellationToken,
                ownerToken);

            if (!claimResult.IsSuccess)
            {
                return Result.Failure<string>(claimResult.Error);
            }

            if (!claimResult.Value)
            {
                return Result.Failure<string>(new RailwayError("A duplicate Kafka message was detected.")
                    .AsInternalServer("Conflict", "Duplicate message detected."));
            }
        }

        return Result.Success(payloadJson);
    }

    private static void EnsureValidIdempotencyMessageId(
        ConsumedMessageHeaders extractedHeaders,
        KafkaTopicConfiguration topicConfig)
    {
        if (!topicConfig.IsIdempotent || extractedHeaders.HasValidMessageIdHeader)
        {
            return;
        }

        throw new InvalidOperationException(
            "Idempotent Kafka consumption requires exactly one valid x-message-id header. " +
            "The message was not processed and its offset was not committed.");
    }

    private Result<string> ResolveAndUpcastMessage<TMessage>(
        string payloadJson,
        string schemaVersionKey)
        where TMessage : class
    {
        if (!MessageVersionResolver.TryResolveSourceSchemaKey(
                schemaVersionKey,
                payloadJson,
                out var sourceKey))
        {
            return Result.Failure<string>(new RailwayError("Kafka source schema version is invalid.")
                .AsBadRequest("Kafka.InvalidSchemaVersion", "Message schema version is invalid."));
        }

        var targetVersion = MessageVersionResolver.GetMessageVersion<TMessage>();
        var targetKey = typeof(TMessage).Name;
        if (!MessageVersionResolver.HasVersionSuffix(targetKey))
        {
            targetKey = $"{targetKey}.v{targetVersion}";
        }

        return _upcastRegistry.UpcastMessage(payloadJson, sourceKey, targetKey);
    }

    /// <summary>
    /// Deserializes a JSON payload into the target message type using Kafka-compatible serializer settings.
    /// </summary>
    private Result<TMessage> DeserializeMessage<TMessage>(string payloadJson) where TMessage : class
    {
        try
        {
            var messageInstance = JsonSerializer.Deserialize<TMessage>(payloadJson, JsonOptions.KafkaCompatible);
            if (messageInstance is null)
            {
                return Result.Failure<TMessage>(new RailwayError("Failed deserializing payload JSON to target message schema model.")
                    .AsBadRequest("ValidationError", "Deserialization returned null."));
            }
            return Result.Success(messageInstance);
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            return Result.Failure<TMessage>(new RailwayError("Kafka payload deserialization failed.")
                .AsBadRequest("Kafka.DeserializationFailed", "Deserialization failed."));
        }
    }

    private Result<TMessage> VerifyMessageVersion<TMessage>(TMessage messageInstance, KafkaTopicConfiguration topicConfig) where TMessage : class
    {
        var topicName = topicConfig.TopicName;
        var topicSegments = topicName.Split('.');
        var topicVersionSegment = topicSegments.Last();
        if (topicVersionSegment.StartsWith("v") && int.TryParse(topicVersionSegment.Substring(1), out var topicVersion))
        {
            var messageVersion = MessageVersionResolver.GetMessageVersion(messageInstance);
            if (messageVersion != topicVersion)
            {
                return Result.Failure<TMessage>(new RailwayError(
                    $"Version mismatch: Message version ({messageVersion}) does not match topic version suffix ({topicVersionSegment}) for topic '{topicName}'.")
                    .AsBadRequest("ValidationError", "VersionMismatch"));
            }
        }
        return Result.Success(messageInstance);
    }

    private async Task<Result> ExecuteHandlerWithRetriesAsync<TMessage, THandler>(
        TMessage messageInstance, 
        KafkaTopicConfiguration topicConfig, 
        CancellationToken cancellationToken)
        where TMessage : class
        where THandler : IMessageHandler<TMessage>
    {
        var maxAttempts = (int)topicConfig.MaxDeliveryAttempts;
        var attempt = 0;
        Result handlerResult = Result.Success();

        while (attempt < maxAttempts)
        {
            attempt++;
            try
            {
                using (var scope = _serviceProvider.CreateScope())
                {
                    var handler = scope.ServiceProvider.GetRequiredService<THandler>();
                    handlerResult = await handler.HandleAsync(messageInstance, cancellationToken);
                    if (handlerResult.IsSuccess)
                    {
                        return Result.Success();
                    }
                    
                    _logger.LogWarning("Handler returned controlled failure (Attempt {Attempt}/{MaxAttempts}). ErrorType: {ErrorType}.",
                        attempt, maxAttempts, handlerResult.Error.Type);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogWarning("Handler threw an exception (Attempt {Attempt}/{MaxAttempts}). ErrorType: {ErrorType}.",
                    attempt, maxAttempts, exception.GetType().Name);
                handlerResult = Result.Failure(new RailwayError("Kafka message handler execution failed.").CausedBy(exception));
            }

            if (attempt < maxAttempts)
            {
                var backoffDelay = CalculateBoundedExponentialDelayMs(
                    _options.Consumer.HandlerRetryBaseDelayMs,
                    _options.Consumer.HandlerRetryMaxDelayMs,
                    attempt);
                await Task.Delay(backoffDelay, cancellationToken);
            }
        }

        return Result.Failure(new RailwayError("Kafka message handler exhausted all delivery attempts.")
            .AsInternalServer("Kafka.HandlerFailed", "Message processing failed."));
    }

    private async Task<Result> ExecuteHandlerWithClaimRenewalAsync<TMessage, THandler>(
        TMessage messageInstance,
        string messageId,
        string consumerGroup,
        string? ownerToken,
        KafkaTopicConfiguration topicConfig,
        CancellationToken cancellationToken)
        where TMessage : class
        where THandler : IMessageHandler<TMessage>
    {
        if (!topicConfig.IsIdempotent || string.IsNullOrWhiteSpace(ownerToken) ||
            _duplicateDetector is not IDistributedDuplicateClaimRenewer renewer)
        {
            return await ExecuteHandlerWithRetriesAsync<TMessage, THandler>(messageInstance, topicConfig, cancellationToken);
        }

        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var operation = ExecuteHandlerWithRetriesAsync<TMessage, THandler>(
            messageInstance,
            topicConfig,
            operationCancellation.Token);
        var renewal = RenewClaimUntilCancelledAsync(
            renewer,
            messageId,
            consumerGroup,
            ownerToken,
            operationCancellation.Token);

        var completed = await Task.WhenAny(operation, renewal);
        if (completed == renewal)
        {
            operationCancellation.Cancel();
            await renewal;
            throw new InvalidOperationException("Idempotency claim renewal stopped unexpectedly.");
        }

        operationCancellation.Cancel();
        try
        {
            await renewal;
        }
        catch (OperationCanceledException) when (operationCancellation.IsCancellationRequested)
        {
        }

        return await operation;
    }

    private async Task<T> WaitWithClaimRenewalAsync<T>(
        Task<T> operation,
        string messageId,
        string consumerGroup,
        string? ownerToken,
        KafkaTopicConfiguration topicConfig,
        CancellationToken cancellationToken)
    {
        if (!topicConfig.IsIdempotent || string.IsNullOrWhiteSpace(ownerToken) ||
            _duplicateDetector is not IDistributedDuplicateClaimRenewer renewer)
        {
            return await operation;
        }

        using var renewalCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var renewalTask = RenewClaimUntilCancelledAsync(
            renewer,
            messageId,
            consumerGroup,
            ownerToken,
            renewalCancellation.Token);

        var completed = await Task.WhenAny(operation, renewalTask);
        if (completed == renewalTask)
        {
            renewalCancellation.Cancel();
            await renewalTask;
            throw new InvalidOperationException("Idempotency claim renewal stopped unexpectedly.");
        }

        renewalCancellation.Cancel();
        try
        {
            await renewalTask;
        }
        catch (OperationCanceledException) when (renewalCancellation.IsCancellationRequested)
        {
        }

        return await operation;
    }

    private async Task RenewClaimUntilCancelledAsync(
        IDistributedDuplicateClaimRenewer renewer,
        string messageId,
        string consumerGroup,
        string ownerToken,
        CancellationToken cancellationToken)
    {
        var intervalSeconds = Math.Clamp(_options.Consumer.MaxPollIntervalMs / 3000, 10, 300);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(intervalSeconds));
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            var result = await renewer.RenewClaimAsync(messageId, consumerGroup, ownerToken, cancellationToken);
            if (!result.IsSuccess)
            {
                throw new InvalidOperationException("The idempotency processing lease was lost.");
            }
        }
    }

    private async Task<Result> RouteToDlqAsync(
        string sourceTopic,
        string dlqTopic,
        string clusterAlias,
        ConsumeResult<string, string> failedMessage,
        Exception exception,
        ConsumedMessageHeaders extractedHeaders,
        KafkaTopicConfiguration topicConfig,
        CancellationToken cancellationToken)
    {
        try
        {
            var headers = new Dictionary<string, string>
            {
                [CustomRequestHeaders.MessageId] = extractedHeaders.MessageId,
                [CustomRequestHeaders.CorrelationId] = extractedHeaders.CorrelationId,
                [CustomRequestHeaders.EventSchemaVersion] = extractedHeaders.SchemaVersionKey,
                [CustomRequestHeaders.OriginalTopic] = sourceTopic,
                [CustomRequestHeaders.OriginalPartition] = failedMessage.Partition.Value.ToString(),
                [CustomRequestHeaders.OriginalOffset] = failedMessage.Offset.Value.ToString(),
                [CustomRequestHeaders.ExceptionType] = SanitizeErrorCode(exception.GetType().Name),
                [CustomRequestHeaders.FailedAtUtc] = DateTime.UtcNow.ToString("O")
            };

            if (topicConfig.EnableAuthentication)
            {
                headers[CustomRequestHeaders.MessageSignature] = _signatureService.SignMessage(
                    failedMessage.Message.Value,
                    extractedHeaders.MessageId,
                    extractedHeaders.CorrelationId,
                    dlqTopic,
                    failedMessage.Message.Key,
                    extractedHeaders.SchemaVersionKey,
                    KafkaEnvelopeKinds.DeadLetter);
            }

            var publishResult = await _producerManager.PublishDirectAsync(
                dlqTopic,
                clusterAlias,
                failedMessage.Message.Key,
                failedMessage.Message.Value,
                headers,
                topicConfig.Username,
                topicConfig.Password,
                cancellationToken);

            return publishResult;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception routingException)
        {
            _logger.LogError("DLQ routing failed. ErrorType: {ErrorType}.", routingException.GetType().Name);
            return Result.Failure("DLQ routing failed.");
        }
    }

    private static string SanitizeErrorCode(string value)
    {
        var safe = new StringBuilder(Math.Min(value.Length, 128));
        foreach (var character in value)
        {
            if (safe.Length >= 128)
            {
                break;
            }

            safe.Append(char.IsAsciiLetterOrDigit(character) || character is '.' or '-' or '_'
                ? character
                : '_');
        }

        return safe.Length == 0 ? "KafkaProcessingFailure" : safe.ToString();
    }

    #endregion

    /// <summary>
    /// Gracefully shuts down all active consumer tasks, waiting up to 30 seconds for completion.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _shutdownCancellation.CancelAsync().ConfigureAwait(false);
        var activeTasks = _consumerTasks.Values.ToArray();
        if (activeTasks.Length == 0)
        {
            _shutdownCancellation.Dispose();
            return;
        }

        _logger.LogInformation("Disposing KafkaMessageBus: waiting for {Count} active consumer task(s) to complete.", activeTasks.Length);

        try
        {
            await Task.WhenAll(activeTasks).WaitAsync(TimeSpan.FromSeconds(30));
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Some consumer tasks did not shut down within the 30-second timeout period.");
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                "Exception observed during consumer task shutdown. ErrorType: {ErrorType}.",
                exception.GetType().Name);
        }

        _consumerTasks.Clear();
        _shutdownCancellation.Dispose();
    }

    private static List<string> ChunkStringByBytes(string text, int maxByteSize)
    {
        var chunks = new List<string>();
        var startIndex = 0;
        while (startIndex < text.Length)
        {
            var endIndex = startIndex;
            var byteCount = 0;
            while (endIndex < text.Length)
            {
                var charLen = char.IsSurrogatePair(text, endIndex) ? 2 : 1;
                var nextBytes = Encoding.UTF8.GetByteCount(text, endIndex, charLen);
                if (byteCount + nextBytes > maxByteSize)
                {
                    break;
                }
                byteCount += nextBytes;
                endIndex += charLen;
            }

            if (endIndex == startIndex)
            {
                endIndex += char.IsSurrogatePair(text, startIndex) ? 2 : 1;
            }

            chunks.Add(text.Substring(startIndex, endIndex - startIndex));
            startIndex = endIndex;
        }
        return chunks;
    }
}
