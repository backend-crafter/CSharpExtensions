namespace CSharpExtensions.Kafka.Abstractions;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Wraps a consumed Kafka message with metadata and acknowledgment controls.
/// Provides at-least-once delivery guarantee through explicit acknowledgment.
/// </summary>
/// <typeparam name="TMessage">The message type.</typeparam>
public sealed class ConsumeContext<TMessage> where TMessage : class
{
    private readonly Func<Task> _acknowledgeAction;
    private readonly Func<string?, Task> _rejectAction;
    private int _completionState;

    /// <summary>
    /// The deserialized message instance.
    /// </summary>
    public TMessage Message { get; }

    /// <summary>
    /// The unique identifier of this Kafka message.
    /// </summary>
    public string MessageId { get; }

    /// <summary>
    /// The correlation identifier for distributed tracing.
    /// </summary>
    public string CorrelationId { get; }

    /// <summary>
    /// The Kafka topic name this message was consumed from.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// The Kafka partition number this message was consumed from.
    /// </summary>
    public int Partition { get; }

    /// <summary>
    /// The Kafka offset of this message within its partition.
    /// </summary>
    public long Offset { get; }

    /// <summary>
    /// The broker-assigned timestamp of this message (UTC).
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    /// All Kafka message headers as a read-only dictionary.
    /// </summary>
    public IReadOnlyDictionary<string, string> Headers { get; }

    internal ConsumeContext(
        TMessage message,
        string messageId,
        string correlationId,
        string topic,
        int partition,
        long offset,
        DateTime timestamp,
        IReadOnlyDictionary<string, string> headers,
        Func<Task> acknowledgeAction,
        Func<string?, Task> rejectAction)
    {
        Message = message ?? throw new ArgumentNullException(nameof(message));
        MessageId = messageId ?? throw new ArgumentNullException(nameof(messageId));
        CorrelationId = correlationId ?? throw new ArgumentNullException(nameof(correlationId));
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));
        Partition = partition;
        Offset = offset;
        Timestamp = timestamp;
        Headers = headers ?? throw new ArgumentNullException(nameof(headers));
        _acknowledgeAction = acknowledgeAction ?? throw new ArgumentNullException(nameof(acknowledgeAction));
        _rejectAction = rejectAction ?? throw new ArgumentNullException(nameof(rejectAction));
    }

    /// <summary>
    /// Acknowledges successful processing. Commits the Kafka offset.
    /// Provides at-least-once delivery guarantee.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the context has already been completed.</exception>
    public async Task AcknowledgeAsync()
    {
        if (Interlocked.Exchange(ref _completionState, 1) != 0)
        {
            throw new InvalidOperationException("ConsumeContext has already been completed.");
        }

        await _acknowledgeAction();
    }

    /// <summary>
    /// Rejects the message. Routes to Dead Letter Queue if configured, then commits offset.
    /// </summary>
    /// <param name="reason">Optional rejection reason for DLQ routing and diagnostics.</param>
    /// <exception cref="InvalidOperationException">Thrown when the context has already been completed.</exception>
    public async Task RejectAsync(string? reason = null)
    {
        if (Interlocked.Exchange(ref _completionState, 1) != 0)
        {
            throw new InvalidOperationException("ConsumeContext has already been completed.");
        }

        await _rejectAction(reason);
    }
}

