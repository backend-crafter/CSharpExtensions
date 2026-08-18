namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Defines access permissions configured for individual topics.
/// </summary>
public enum TopicPermission
{
    Read,
    Write,
    ReadWrite
}
