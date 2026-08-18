namespace CSharpExtensions.Kafka.Core;

using System;
using CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Fluent builder for configuring individual Kafka message subscriptions.
/// Passed as a delegate parameter inside <c>Subscribe&lt;TMessage&gt;</c>.
/// </summary>
/// <typeparam name="TMessage">The message type being subscribed to.</typeparam>
public sealed class KafkaSubscriptionBuilder<TMessage> where TMessage : class
{
    internal Type? HandlerType { get; private set; }
    internal bool HasUpcastChainResolver { get; private set; }
    internal UpcasterGenerationMode UpcastMode { get; private set; }

    /// <summary>
    /// Controls the starting offset for the consumer.
    /// </summary>
    public KafkaReadMode ReadMode { get; set; } = KafkaReadMode.Latest;

    /// <summary>
    /// Overrides the consumer group from topic configuration.
    /// When empty, falls back to <see cref="KafkaTopicConfiguration.GroupId"/>.
    /// </summary>
    public string ConsumerGroup { get; set; } = "";

    /// <summary>
    /// Starting offset timestamp (ISO 8601) or numeric offset for historical replay.
    /// Only used when <see cref="ReadMode"/> is <see cref="KafkaReadMode.HistoricalReplay"/>.
    /// </summary>
    public string StartOffsetTime { get; set; } = "";

    /// <summary>
    /// Registers a message handler for automatic processing.
    /// The handler is registered as Scoped in DI and invoked for each consumed message.
    /// When no handler is registered, use <see cref="IKafkaConsumer{TMessage}"/> for manual consumption.
    /// </summary>
    /// <typeparam name="THandler">The handler implementation type.</typeparam>
    public KafkaSubscriptionBuilder<TMessage> AddHandler<THandler>()
        where THandler : class, IMessageHandler<TMessage>
    {
        HandlerType = typeof(THandler);
        return this;
    }

    /// <summary>
    /// Enables build-time schema evolution chain generation for this subscription.
    /// Acts only as a marker for the explicitly enabled CLI analyzer.
    /// Generated runtime services must be registered through the generated DI extension.
    /// </summary>
    /// <param name="mode">Controls whether to regenerate or skip if files already exist.</param>
    public KafkaSubscriptionBuilder<TMessage> AddUpcastChainResolver(UpcasterGenerationMode mode)
    {
        HasUpcastChainResolver = true;
        UpcastMode = mode;
        return this;
    }
}
