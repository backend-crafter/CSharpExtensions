namespace CSharpExtensions.Kafka.Core;

using System;
using CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Internal descriptor that captures subscription configuration registered via the builder.
/// </summary>
internal sealed class MessageSubscriptionDescriptor
{
    /// <summary>
    /// The message type (e.g., EligibleWagerFactRecorded).
    /// </summary>
    public Type MessageType { get; }

    /// <summary>
    /// The handler implementation type. Null when using handler-less consumer mode.
    /// </summary>
    public Type? HandlerType { get; }

    /// <summary>
    /// Subscription options (consumer group, read mode, etc.).
    /// </summary>
    public KafkaSubscriptionOptions Options { get; }

    /// <summary>
    /// The subscription mode: handler-based or manual consumer.
    /// </summary>
    public SubscriptionMode Mode { get; }

    public MessageSubscriptionDescriptor(
        Type messageType,
        Type? handlerType,
        KafkaSubscriptionOptions options,
        SubscriptionMode mode)
    {
        MessageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
        Options = options ?? throw new ArgumentNullException(nameof(options));
        Mode = mode;
        HandlerType = handlerType;
    }
}

/// <summary>
/// Defines how a subscription processes messages.
/// </summary>
internal enum SubscriptionMode
{
    /// <summary>
    /// Messages are automatically dispatched to a registered <see cref="IMessageHandler{TMessage}"/>.
    /// </summary>
    Handler,

    /// <summary>
    /// Messages are written to a channel for manual consumption via <see cref="IKafkaConsumer{TMessage}"/>.
    /// </summary>
    Consumer
}
