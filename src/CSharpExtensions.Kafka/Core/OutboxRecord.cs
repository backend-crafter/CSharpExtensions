namespace CSharpExtensions.Kafka.Core;

/// <summary>
/// Represents a row read from the local outbox table.
/// </summary>
public sealed record OutboxRecord(
    long OutboxId,
    string MessageId,
    string CorrelationId,
    string ConfigurationKey,
    string? MessageKey,
    string PayloadJson,
    string? HeadersJson,
    string ProcessingStatus,
    int AttemptCount,
    int MaxAttempts,
    string ProcessingOwner,
    long ClaimVersion)
{
    /// <summary>
    /// Creates an outbox record using the legacy pre-lease shape.
    /// </summary>
    public OutboxRecord(
        long OutboxId,
        string MessageId,
        string CorrelationId,
        string ConfigurationKey,
        string? MessageKey,
        string PayloadJson,
        string? HeadersJson,
        string ProcessingStatus,
        int AttemptCount,
        int MaxAttempts)
        : this(
            OutboxId,
            MessageId,
            CorrelationId,
            ConfigurationKey,
            MessageKey,
            PayloadJson,
            HeadersJson,
            ProcessingStatus,
            AttemptCount,
            MaxAttempts,
            string.Empty,
            0)
    {
    }

    /// <summary>
    /// Deconstructs the record using the legacy pre-lease shape.
    /// </summary>
    public void Deconstruct(
        out long OutboxId,
        out string MessageId,
        out string CorrelationId,
        out string ConfigurationKey,
        out string? MessageKey,
        out string PayloadJson,
        out string? HeadersJson,
        out string ProcessingStatus,
        out int AttemptCount,
        out int MaxAttempts)
    {
        OutboxId = this.OutboxId;
        MessageId = this.MessageId;
        CorrelationId = this.CorrelationId;
        ConfigurationKey = this.ConfigurationKey;
        MessageKey = this.MessageKey;
        PayloadJson = this.PayloadJson;
        HeadersJson = this.HeadersJson;
        ProcessingStatus = this.ProcessingStatus;
        AttemptCount = this.AttemptCount;
        MaxAttempts = this.MaxAttempts;
    }
}
