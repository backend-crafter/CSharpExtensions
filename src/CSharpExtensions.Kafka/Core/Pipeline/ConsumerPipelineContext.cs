namespace CSharpExtensions.Kafka.Core.Pipeline;

using System;
using System.Collections.Generic;
using CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Internal context object passed through the consumer pipeline.
/// Carries the raw payload and metadata through each processing step.
/// </summary>
public sealed class ConsumerPipelineContext
{
    /// <summary>Raw or transformed payload JSON string.</summary>
    public string RawPayload { get; set; } = "";

    /// <summary>Unique message identifier from x-message-id header.</summary>
    public string MessageId { get; set; } = "";

    /// <summary>Correlation identifier for distributed tracing.</summary>
    public string CorrelationId { get; set; } = "";

    /// <summary>Schema version key from x-event-schema-version header.</summary>
    public string SchemaVersionKey { get; set; } = "";

    /// <summary>Cryptographic signature from x-message-signature header (nullable).</summary>
    public string? Signature { get; set; }

    /// <summary>W3C traceparent header value (nullable).</summary>
    public string? Traceparent { get; set; }

    /// <summary>Topic configuration for the consumed message.</summary>
    public required KafkaTopicConfiguration TopicConfig { get; init; }

    /// <summary>Consumer group processing this message.</summary>
    public string ConsumerGroup { get; set; } = "";

    // Kafka transport metadata

    /// <summary>Kafka topic name.</summary>
    public string TopicName { get; set; } = "";

    /// <summary>Kafka record key, preserving the distinction between null and an empty key.</summary>
    public string? MessageKey { get; set; }

    /// <summary>Kafka partition number.</summary>
    public int Partition { get; set; }

    /// <summary>Kafka offset within the partition.</summary>
    public long Offset { get; set; }

    /// <summary>Broker-assigned message timestamp (UTC).</summary>
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;

    /// <summary>All Kafka message headers as read-only dictionary.</summary>
    public IReadOnlyDictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();

    /// <summary>The strongly-typed deserialized message object, set by DeserializationStep.</summary>
    public object? Message { get; set; }
}
