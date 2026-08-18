using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Tests.Kafka;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

[Topic("TestTopic")]
public class TestRecoveryMessage
{
    public string Id { get; set; } = "";
}

public sealed class KafkaRecoveryTests
{
    private sealed class NoopRecoveryLease : IKafkaRecoveryLease
    {
        public CancellationToken LeaseLostToken => CancellationToken.None;

        public bool IsLost => false;

        public void ThrowIfLost()
        {
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NoopRecoveryLockProvider : IKafkaRecoveryLockProvider
    {
        public Task<IKafkaRecoveryLease> AcquireAsync(
            string connectionString,
            IEnumerable<string> protectedTopicNames,
            int lockTimeoutMs,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult<IKafkaRecoveryLease>(new NoopRecoveryLease());
        }
    }

    private readonly Mock<IDbStagedRepairPipeline> _mockPipeline;
    private readonly ILogger<KafkaRecoveryManager> _logger;
    private readonly KafkaOptions _kafkaOptions;

    public KafkaRecoveryTests()
    {
        _mockPipeline = new Mock<IDbStagedRepairPipeline>();
        _logger = new NullLoggerFactory().CreateLogger<KafkaRecoveryManager>();
        _kafkaOptions = new KafkaOptions
        {
            Servers = "localhost:9092",
            Topics = new Dictionary<string, KafkaTopicConfiguration>
            {
                {
                    "TestRecoveryMessage",
                    new KafkaTopicConfiguration { TopicName = "test-topic" }
                }
            }
        };
    }

    [Fact]
    public async Task StartAllRecoveries_ExecutesPipelinePhasesAndReportsProgress()
    {
        // Arrange
        var settings = new KafkaRepairSettings
        {
            ConnectionStringName = "TestConnection",
            TableSchema = "dbo",
            ExportBatchSize = 10,
            UpcastBatchSize = 10,
            RepopulateBatchSize = 5
        };

        var inMemorySettings = new Dictionary<string, string?>
        {
            { "ConnectionStrings:TestConnection", "Server=localhost;Database=Test;Trusted_Connection=True;" }
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(inMemorySettings)
            .Build();

        var config = new KafkaRepairConfiguration(typeof(TestRecoveryMessage), "TestRecoveryMessage", settings);

        // Mock Pipeline methods
        _mockPipeline
            .Setup(p => p.ExportToStagingAsync(
                "test-topic",
                It.IsAny<string>(),
                It.IsAny<Action<long, long>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, Action<long, long>, CancellationToken>((t, conn, progress, ct) =>
            {
                progress?.Invoke(5, 10);
            })
            .ReturnsAsync(Result.Success(10L));

        _mockPipeline
            .Setup(p => p.RepopulateTopicFromStagingAsync(
                "test-topic",
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<Action<long, long>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, int, Action<long, long>, CancellationToken>((t, conn, bs, progress, ct) =>
            {
                progress?.Invoke(8, 10);
            })
            .ReturnsAsync(Result.Success());

        // Upcast is invoked via reflection, so we need to set it up dynamically
        _mockPipeline
            .Setup(p => p.UpcastStagedPayloadsAsync<TestRecoveryMessage>(
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<Action<long, long>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, int, Action<long, long>, CancellationToken>((conn, bs, progress, ct) =>
            {
                progress?.Invoke(3, 10);
            })
            .ReturnsAsync(Result.Success());

        using var manager = new KafkaRecoveryManager(
            _mockPipeline.Object,
            new[] { config },
            Options.Create(_kafkaOptions),
            configuration,
            _logger,
            new NoopRecoveryLockProvider());

        // Act
        var result = manager.StartAllRecoveries();

        // Assert
        Assert.True(result.IsSuccess);

        await manager.WaitForActiveRecoveriesAsync().WaitAsync(TimeSpan.FromSeconds(5));
        var status = manager.GetStatuses().First();

        Assert.Equal("Completed", status.Phase);
        Assert.Null(status.ErrorMessage);

        // Verify progress reporting captured values
        _mockPipeline.Verify(p => p.ExportToStagingAsync(
            "test-topic",
            It.IsAny<string>(),
            It.IsAny<Action<long, long>>(),
            It.IsAny<CancellationToken>()), Times.Once);

        _mockPipeline.Verify(p => p.UpcastStagedPayloadsAsync<TestRecoveryMessage>(
            It.IsAny<string>(),
            It.IsAny<int>(),
            It.IsAny<Action<long, long>>(),
            It.IsAny<CancellationToken>()), Times.Once);

        _mockPipeline.Verify(p => p.RepopulateTopicFromStagingAsync(
            "test-topic",
            It.IsAny<string>(),
            It.IsAny<int>(),
            It.IsAny<Action<long, long>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartAllRecoveries_ConnectionStringNotFound_FailsJob()
    {
        // Arrange
        var settings = new KafkaRepairSettings
        {
            ConnectionStringName = "NonexistentConnection",
            TableSchema = "dbo"
        };

        var inMemorySettings = new Dictionary<string, string?>();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(inMemorySettings)
            .Build();

        var config = new KafkaRepairConfiguration(typeof(TestRecoveryMessage), "TestRecoveryMessage", settings);

        using var manager = new KafkaRecoveryManager(
            _mockPipeline.Object,
            new[] { config },
            Options.Create(_kafkaOptions),
            configuration,
            _logger,
            new NoopRecoveryLockProvider());

        // Act
        var result = manager.StartAllRecoveries();

        // Assert
        Assert.True(result.IsSuccess);

        await manager.WaitForActiveRecoveriesAsync().WaitAsync(TimeSpan.FromSeconds(5));
        var status = manager.GetStatuses().First();

        Assert.Equal("Failed", status.Phase);
        Assert.Equal("The recovery database connection is unavailable.", status.ErrorMessage);
    }

    [Fact]
    public async Task StartAllRecoveries_DoesNotStartASecondJobWhilePendingOrRunning()
    {
        var settings = new KafkaRepairSettings
        {
            ConnectionStringName = "TestConnection",
            TableSchema = "dbo"
        };
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:TestConnection"] = "Server=localhost;Database=Test;Trusted_Connection=True;"
            })
            .Build();
        var config = new KafkaRepairConfiguration(
            typeof(TestRecoveryMessage),
            "TestRecoveryMessage",
            settings);
        var exportStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseExport = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        _mockPipeline
            .Setup(p => p.ExportToStagingAsync(
                "test-topic",
                It.IsAny<string>(),
                It.IsAny<Action<long, long>>(),
                It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                exportStarted.TrySetResult();
                await releaseExport.Task;
                return Result.Success(0L);
            });
        _mockPipeline
            .Setup(p => p.UpcastStagedPayloadsAsync<TestRecoveryMessage>(
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<Action<long, long>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Success());
        _mockPipeline
            .Setup(p => p.RepopulateTopicFromStagingAsync(
                "test-topic",
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<Action<long, long>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Success());

        using var manager = new KafkaRecoveryManager(
            _mockPipeline.Object,
            new[] { config },
            Options.Create(_kafkaOptions),
            configuration,
            _logger,
            new NoopRecoveryLockProvider());

        Assert.True(manager.StartAllRecoveries().IsSuccess);
        Assert.True(manager.StartAllRecoveries().IsSuccess);
        await exportStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        _mockPipeline.Verify(p => p.ExportToStagingAsync(
            "test-topic",
            It.IsAny<string>(),
            It.IsAny<Action<long, long>>(),
            It.IsAny<CancellationToken>()), Times.Once);

        releaseExport.TrySetResult();
        await manager.WaitForActiveRecoveriesAsync().WaitAsync(TimeSpan.FromSeconds(5));
    }
}
