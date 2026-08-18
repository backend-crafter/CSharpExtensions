using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Internal hosted service that automatically starts all message subscriptions registered via
/// <see cref="KafkaBuilder.Subscribe{TMessage}"/> when the application starts.
/// Cancels all consumer loops on application shutdown.
/// </summary>
internal sealed class KafkaSubscriptionHostedService(
    KafkaMessageBus messageBus,
    IServiceProvider serviceProvider,
    IReadOnlyList<MessageSubscriptionDescriptor> subscriptions,
    ILogger<KafkaSubscriptionHostedService> logger)
    : IHostedService, IDisposable
{
    private readonly KafkaMessageBus _messageBus = messageBus ?? throw new ArgumentNullException(nameof(messageBus));
    private readonly IServiceProvider _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private readonly IReadOnlyList<MessageSubscriptionDescriptor> _subscriptions = subscriptions ?? throw new ArgumentNullException(nameof(subscriptions));
    private readonly ILogger<KafkaSubscriptionHostedService> _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private CancellationTokenSource? _stoppingTokenSource;

    private static readonly MethodInfo SubscribeWithHandlerMethod =
        typeof(KafkaMessageBus).GetMethod(nameof(KafkaMessageBus.SubscribeWithHandlerAsync),
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static readonly MethodInfo SubscribeConsumerMethod =
        typeof(KafkaMessageBus).GetMethod(nameof(KafkaMessageBus.SubscribeConsumerAsync),
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _stoppingTokenSource = new CancellationTokenSource();

        _logger.LogInformation("Starting {Count} Kafka message subscription(s).", _subscriptions.Count);

        var options = _serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<KafkaOptions>>().Value;

        foreach (var subscription in _subscriptions)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Validate that the message version matches the topic version suffix
            var configKey = TopicAttributeResolver.Resolve(subscription.MessageType);
            if (options.Topics.TryGetValue(configKey, out var topicConfig))
            {
                var topicName = topicConfig.TopicName;
                var topicSegments = topicName.Split('.');
                var topicVersionSegment = topicSegments.Last();
                if (topicVersionSegment.StartsWith("v") && int.TryParse(topicVersionSegment.Substring(1), out var topicVersion))
                {
                    var messageVersion = MessageVersionResolver.GetMessageVersion(subscription.MessageType);
                    if (messageVersion != topicVersion)
                    {
                        throw new InvalidOperationException(
                            $"Topic version mismatch for message type '{subscription.MessageType.Name}'. " +
                            $"The message has Version => {messageVersion}, but it is configured to use topic '{topicName}' which has version suffix '{topicVersionSegment}'. They must be equal.");
                    }
                }
            }

            try
            {
                if (subscription is { Mode: SubscriptionMode.Handler, HandlerType: not null })
                {
                    var method = SubscribeWithHandlerMethod.MakeGenericMethod(
                        subscription.MessageType, subscription.HandlerType);

                    var resultTask = (Task<Result>)method.Invoke(
                        _messageBus, [subscription.Options, _stoppingTokenSource.Token])!;

                    var result = await resultTask;
                    
                    if (result.IsSuccess)
                    {
                        _logger.LogInformation(
                            "Subscribed to '{MessageType}' via handler '{HandlerType}'.",
                            subscription.MessageType.Name, subscription.HandlerType.Name);
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            $"Failed to start Kafka subscription for '{subscription.MessageType.FullName}'.");
                    }
                }
                else if (subscription.Mode == SubscriptionMode.Consumer)
                {
                    var method = SubscribeConsumerMethod.MakeGenericMethod(subscription.MessageType);

                    var resultTask = (Task<Result>)method.Invoke(
                        _messageBus, [subscription.Options, _stoppingTokenSource.Token])!;

                    var result = await resultTask;
                    if (result.IsSuccess)
                    {
                        _logger.LogInformation(
                            "Subscribed to '{MessageType}' in consumer mode (IKafkaConsumer).",
                            subscription.MessageType.Name);
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            $"Failed to start Kafka consumer subscription for '{subscription.MessageType.FullName}'.");
                    }
                }
                else
                {
                    throw new InvalidOperationException(
                        $"Subscription for '{subscription.MessageType.FullName}' has an invalid mode or missing handler type.");
                }
            }
            catch (Exception exception)
            {
                await _stoppingTokenSource.CancelAsync();
                var startupException = exception is TargetInvocationException { InnerException: not null }
                    ? exception.InnerException
                    : exception;
                _logger.LogCritical(
                    "Kafka subscription startup failed for '{MessageType}'. Application startup will be aborted. ErrorType: {ErrorType}.",
                    subscription.MessageType.Name,
                    startupException?.GetType().Name);
                throw new InvalidOperationException(
                    $"Kafka subscription startup failed for '{subscription.MessageType.FullName}'.");
            }
        }

        _logger.LogInformation("All Kafka message subscriptions started.");
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping Kafka message subscriptions.");
        _stoppingTokenSource?.Cancel();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _stoppingTokenSource?.Dispose();
    }
}
