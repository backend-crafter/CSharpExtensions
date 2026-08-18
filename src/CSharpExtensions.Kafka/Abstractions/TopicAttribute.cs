namespace CSharpExtensions.Kafka.Abstractions;

using System;

/// <summary>
/// Decorates event classes to map them to topic registry configurations.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class TopicAttribute : Attribute
{
    public string? ConfigurationKey { get; }

    /// <summary>
    /// Decorates an event class. If configurationKey is null, the C# class name is used.
    /// </summary>
    public TopicAttribute(string? configurationKey = null)
    {
        ConfigurationKey = configurationKey;
    }
}
