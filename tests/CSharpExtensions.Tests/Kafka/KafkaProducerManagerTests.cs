namespace CSharpExtensions.Tests.Kafka;

using System.Collections.Concurrent;
using Confluent.Kafka;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

public sealed class KafkaProducerManagerTests
{
    [Fact]
    public async Task ConcurrentDistinctCredentials_NeverExceedConfiguredCacheCapacity()
    {
        const int capacity = 8;
        var options = new KafkaOptions
        {
            DefaultClusterAlias = "Default",
            Clusters = new Dictionary<string, KafkaClusterConfiguration>
            {
                ["Default"] = new() { BootstrapServers = "localhost:9092" }
            }
        };
        options.Producer.MaxCachedProducers = capacity;

        var createdCount = 0;
        var accepted = new ConcurrentBag<(string Username, string Password, IProducer<string, string> Producer)>();
        using var manager = new KafkaProducerManager(
            Options.Create(options),
            Mock.Of<ILogger<KafkaProducerManager>>(),
            config =>
            {
                var sequence = Interlocked.Increment(ref createdCount);
                var producer = new Mock<IProducer<string, string>>();
                producer.SetupGet(instance => instance.Name).Returns($"producer-{sequence}");
                return producer.Object;
            });

        var attempts = Enumerable.Range(0, 64)
            .Select(index => Task.Run(() =>
            {
                var username = $"user-{index}";
                var password = $"password-{index}";
                try
                {
                    var producer = manager.GetOrCreateProducer("Default", username, password);
                    accepted.Add((username, password, producer));
                }
                catch (InvalidOperationException exception)
                {
                    Assert.Contains("cache capacity", exception.Message, StringComparison.Ordinal);
                }
            }));

        await Task.WhenAll(attempts);

        Assert.Equal(capacity, manager.CachedProducerCount);
        Assert.Equal(capacity, accepted.Count);
        Assert.Equal(capacity, Volatile.Read(ref createdCount));

        var existing = accepted.First();
        var resolvedAgain = manager.GetOrCreateProducer(
            "Default",
            existing.Username,
            existing.Password);

        Assert.Same(existing.Producer, resolvedAgain);
        Assert.False(string.IsNullOrWhiteSpace(resolvedAgain.Name));
        Assert.Equal(capacity, manager.CachedProducerCount);
    }
}
