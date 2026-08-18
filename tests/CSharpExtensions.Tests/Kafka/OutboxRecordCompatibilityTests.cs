using CSharpExtensions.Kafka.Core;

namespace CSharpExtensions.Tests.Kafka;

using Xunit;

public sealed class OutboxRecordCompatibilityTests
{
    [Fact]
    public void LegacyConstructorAndDeconstructRemainAvailable()
    {
        var record = new OutboxRecord(
            OutboxId: 1,
            MessageId: "message",
            CorrelationId: "correlation",
            ConfigurationKey: "configuration",
            MessageKey: "key",
            PayloadJson: "{}",
            HeadersJson: null,
            ProcessingStatus: "Pending",
            AttemptCount: 2,
            MaxAttempts: 5);

        var (outboxId, messageId, correlationId, configurationKey, messageKey,
            payloadJson, headersJson, processingStatus, attemptCount, maxAttempts) = record;

        Assert.Equal(1, outboxId);
        Assert.Equal("message", messageId);
        Assert.Equal("correlation", correlationId);
        Assert.Equal("configuration", configurationKey);
        Assert.Equal("key", messageKey);
        Assert.Equal("{}", payloadJson);
        Assert.Null(headersJson);
        Assert.Equal("Pending", processingStatus);
        Assert.Equal(2, attemptCount);
        Assert.Equal(5, maxAttempts);
        Assert.Equal(string.Empty, record.ProcessingOwner);
        Assert.Equal(0, record.ClaimVersion);

        Assert.NotNull(typeof(OutboxRecord).GetConstructor(
        [
            typeof(long),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(int),
            typeof(int)
        ]));
    }
}
