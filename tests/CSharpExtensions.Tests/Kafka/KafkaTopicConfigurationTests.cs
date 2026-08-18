namespace CSharpExtensions.Tests.Kafka;

using CSharpExtensions.Kafka.Abstractions;
using Xunit;

public sealed class KafkaTopicConfigurationTests
{
    [Fact]
    public void LargePayloadStrategyIsDisabledByDefault()
    {
        var configuration = new KafkaTopicConfiguration();

        Assert.Null(configuration.ResolvedStrategy);
    }

    [Theory]
    [InlineData(LargePayloadStrategy.S3Offloading)]
    [InlineData(LargePayloadStrategy.Segmenting)]
    public void ExplicitLargePayloadStrategyIsPreserved(LargePayloadStrategy strategy)
    {
        var configuration = new KafkaTopicConfiguration
        {
            LargePayloadStrategy = strategy
        };

        Assert.Equal(strategy, configuration.ResolvedStrategy);
    }
}
