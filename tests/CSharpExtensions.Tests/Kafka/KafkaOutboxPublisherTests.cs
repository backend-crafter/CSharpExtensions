using CSharpExtensions.Kafka.Core;

namespace CSharpExtensions.Tests.Kafka;

using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

public sealed class KafkaOutboxPublisherTests
{
    [Fact]
    public void InsertSqlUsesConfiguredValidatedSchema()
    {
        var sql = KafkaOutboxPublisher.BuildInsertSql("messaging");

        Assert.Contains("INSERT INTO [messaging].kafka_outbox", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT INTO dbo.kafka_outbox", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("dbo]; DROP TABLE kafka_outbox;--")]
    [InlineData("schema.with.dot")]
    [InlineData("")]
    public void InsertSqlRejectsUnsafeSchema(string schema)
    {
        Assert.ThrowsAny<ArgumentException>(() => KafkaOutboxPublisher.BuildInsertSql(schema));
    }

    [Fact]
    public void ConstructorReadsSchemaFromKafkaOptions()
    {
        var options = new KafkaOptions();
        options.Outbox.TableSchema = "events";

        var publisher = new KafkaOutboxPublisher(Options.Create(options));

        Assert.NotNull(publisher);
    }

    [Fact]
    public void EnqueueBounds_RejectDatabaseAndConfiguredMessageKeyOverflows()
    {
        Assert.False(KafkaOutboxPublisher.IsMessageKeyWithinLimits(new string('a', 501), 4096));
        Assert.False(KafkaOutboxPublisher.IsMessageKeyWithinLimits("éé", 3));
        Assert.True(KafkaOutboxPublisher.IsMessageKeyWithinLimits(new string('a', 500), 500));
    }

    [Fact]
    public void EnqueueBounds_RejectOversizedPayloadBeforeInsert()
    {
        Assert.False(KafkaOutboxPublisher.IsPayloadWithinLimit("{\"value\":\"oversized\"}", 8));
        Assert.True(KafkaOutboxPublisher.IsPayloadWithinLimit("{}", 2));
    }

    [Fact]
    public void EnqueueBounds_RejectUnsafeOrOversizedConfigurationKey()
    {
        Assert.False(KafkaOutboxPublisher.TryNormalizeConfigurationKey("bad key", out _));
        Assert.False(KafkaOutboxPublisher.TryNormalizeConfigurationKey(new string('a', 201), out _));
        Assert.True(KafkaOutboxPublisher.TryNormalizeConfigurationKey("EventsOrdersOrderPlacedV1", out var normalized));
        Assert.Equal("EventsOrdersOrderPlacedV1", normalized);
    }
}
