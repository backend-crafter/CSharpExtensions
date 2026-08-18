namespace CSharpExtensions.Kafka.Abstractions;

using System;

/// <summary>
/// Defines a registered topic repair configuration mapping a target message type to its repair settings.
/// </summary>
public sealed class KafkaRepairConfiguration
{
    /// <summary>
    /// Gets the .NET type of the message payload to be repaired.
    /// </summary>
    public Type TargetMessageType { get; }

    /// <summary>
    /// Gets the Kafka topic configuration key associated with the message type.
    /// </summary>
    public string TopicConfigurationKey { get; }

    /// <summary>
    /// Gets the settings for this specific topic repair process.
    /// </summary>
    public KafkaRepairSettings Settings { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaRepairConfiguration"/> class.
    /// </summary>
    /// <param name="targetMessageType">The target message type.</param>
    /// <param name="topicConfigurationKey">The topic configuration key.</param>
    /// <param name="settings">The repair settings.</param>
    public KafkaRepairConfiguration(Type targetMessageType, string topicConfigurationKey, KafkaRepairSettings settings)
    {
        TargetMessageType = targetMessageType ?? throw new ArgumentNullException(nameof(targetMessageType));
        TopicConfigurationKey = topicConfigurationKey ?? throw new ArgumentNullException(nameof(topicConfigurationKey));
        Settings = settings ?? throw new ArgumentNullException(nameof(settings));
    }
}
