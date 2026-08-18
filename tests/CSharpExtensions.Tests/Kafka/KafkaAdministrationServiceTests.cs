namespace CSharpExtensions.Tests.Kafka;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

/// <summary>
/// Unit tests for <see cref="KafkaAdministrationService"/>.
/// Focuses on input validation and constructor guards that can be tested without a real Kafka broker.
/// Uses NullLogger instead of Moq because KafkaAdministrationService is internal and Castle.DynamicProxy
/// cannot create a proxy for ILogger of an internal generic type argument.
/// </summary>
public class KafkaAdministrationServiceTests
{
    private static readonly ILogger<KafkaAdministrationService> Logger =
        NullLoggerFactory.Instance.CreateLogger<KafkaAdministrationService>();

    private static KafkaAdministrationService CreateService(KafkaOptions? kafkaOptions = null)
    {
        var options = kafkaOptions ?? new KafkaOptions
        {
            Servers = "localhost:9092",
            Clusters = new Dictionary<string, KafkaClusterConfiguration>
            {
                { "Default", new KafkaClusterConfiguration { BootstrapServers = "localhost:9092" } }
            }
        };

        return new KafkaAdministrationService(Options.Create(options), Logger);
    }

    // ──────────────────────────────────────────────────────────
    // Constructor null checks
    // ──────────────────────────────────────────────────────────

    [Fact]
    public void Constructor_NullOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(
            () => new KafkaAdministrationService(null!, Logger));
    }

    [Fact]
    public void Constructor_NullLogger_ThrowsArgumentNullException()
    {
        // Arrange
        var options = Options.Create(new KafkaOptions { Servers = "localhost:9092" });

        // Act & Assert
        Assert.Throws<ArgumentNullException>(
            () => new KafkaAdministrationService(options, null!));
    }

    // ──────────────────────────────────────────────────────────
    // CreateTopicAsync — input validation
    // ──────────────────────────────────────────────────────────

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task CreateTopicAsync_EmptyTopicName_ReturnsFailure(string? topicName)
    {
        // Arrange
        var service = CreateService();

        // Act
        var result = await service.CreateTopicAsync(
            topicName!,
            partitionCount: 3,
            replicationFactor: 1,
            cancellationToken: CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Contains("empty", result.Error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CreateTopicAsync_CancelledToken_ThrowsOperationCancelledException()
    {
        // Arrange
        var service = CreateService();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => service.CreateTopicAsync(
                "test.events.entity.created",
                partitionCount: 3,
                replicationFactor: 1,
                cancellationToken: cancellationTokenSource.Token));
    }

    // ──────────────────────────────────────────────────────────
    // DeleteTopicAsync — input validation
    // ──────────────────────────────────────────────────────────

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task DeleteTopicAsync_EmptyTopicName_ReturnsFailure(string? topicName)
    {
        // Arrange
        var service = CreateService();

        // Act
        var result = await service.DeleteTopicAsync(
            topicName!,
            cancellationToken: CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Contains("empty", result.Error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DeleteTopicAsync_CancelledToken_ThrowsOperationCancelledException()
    {
        // Arrange
        var service = CreateService();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => service.DeleteTopicAsync(
                "test.events.entity.created",
                cancellationToken: cancellationTokenSource.Token));
    }

    // ──────────────────────────────────────────────────────────
    // GetTopicMetadataAsync — input validation
    // ──────────────────────────────────────────────────────────

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task GetTopicMetadataAsync_EmptyTopicName_ReturnsFailure(string? topicName)
    {
        // Arrange
        var service = CreateService();

        // Act
        var result = await service.GetTopicMetadataAsync(
            topicName!,
            cancellationToken: CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Contains("empty", result.Error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task GetTopicMetadataAsync_CancelledToken_ThrowsOperationCancelledException()
    {
        // Arrange
        var service = CreateService();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => service.GetTopicMetadataAsync(
                "test.events.entity.created",
                cancellationToken: cancellationTokenSource.Token));
    }

    // ──────────────────────────────────────────────────────────
    // BuildAdminClient — cluster alias resolution
    // ──────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateTopicAsync_UnknownClusterWithoutFallback_ReturnsFailure()
    {
        // Arrange — no Servers fallback, no matching cluster
        var options = new KafkaOptions
        {
            Servers = "",
            DefaultClusterAlias = "Default",
            Clusters = new Dictionary<string, KafkaClusterConfiguration>()
        };
        var service = CreateService(options);

        // Act
        var result = await service.CreateTopicAsync(
            "test.events.entity.created",
            partitionCount: 3,
            replicationFactor: 1,
            clusterAlias: "NonExistent",
            cancellationToken: CancellationToken.None);

        // Assert — the BuildAdminClient throws InvalidOperationException, which is caught
        // by the generic Exception handler and returned as a failure Result
        Assert.True(result.IsFailure);
        Assert.Equal("Kafka topic creation failed.", result.Error.Message);
    }

    [Fact]
    public async Task DeleteTopicAsync_UnknownClusterWithoutFallback_ReturnsFailure()
    {
        // Arrange
        var options = new KafkaOptions
        {
            Servers = "",
            DefaultClusterAlias = "Default",
            Clusters = new Dictionary<string, KafkaClusterConfiguration>()
        };
        var service = CreateService(options);

        // Act
        var result = await service.DeleteTopicAsync(
            "test.events.entity.created",
            clusterAlias: "NonExistent",
            cancellationToken: CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Equal("Kafka topic deletion failed.", result.Error.Message);
    }

    [Fact]
    public async Task GetTopicMetadataAsync_UnknownClusterWithoutFallback_ReturnsFailure()
    {
        // Arrange
        var options = new KafkaOptions
        {
            Servers = "",
            DefaultClusterAlias = "Default",
            Clusters = new Dictionary<string, KafkaClusterConfiguration>()
        };
        var service = CreateService(options);

        // Act
        var result = await service.GetTopicMetadataAsync(
            "test.events.entity.created",
            clusterAlias: "NonExistent",
            cancellationToken: CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Equal("Kafka topic metadata request failed.", result.Error.Message);
    }
}
