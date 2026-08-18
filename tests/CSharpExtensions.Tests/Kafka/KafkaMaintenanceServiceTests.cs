using CSharpExtensions.Foundation.Railway;
using CSharpExtensions.Foundation.Security.Interfaces;

namespace CSharpExtensions.Tests.Kafka;

using System.Collections.Generic;
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

/// <summary>
/// Unit tests for the internal <see cref="KafkaMaintenanceService"/> implementation.
/// Validates early-return guard logic and delegation to dependency services.
/// </summary>
public sealed class KafkaMaintenanceServiceTests
{
    private readonly Mock<IKafkaAdministrationService> _mockAdministrationService;
    private readonly Mock<IKafkaTopicValidator> _mockTopicValidator;
    private readonly ILogger<KafkaMaintenanceService> _logger;
    private readonly Mock<IConfiguration> _mockConfiguration;
    private readonly KafkaProducerManager _producerManager;

    public KafkaMaintenanceServiceTests()
    {
        _mockAdministrationService = new Mock<IKafkaAdministrationService>();
        _mockTopicValidator = new Mock<IKafkaTopicValidator>();
        _logger = new NullLoggerFactory().CreateLogger<KafkaMaintenanceService>();
        _mockConfiguration = new Mock<IConfiguration>();

        // KafkaProducerManager is sealed, so we create a real instance with minimal options.
        // Tests that exercise early-return paths never reach the producer logic.
        var producerOptions = Options.Create(new KafkaOptions { Servers = "localhost:9092" });
        var producerLogger = new Mock<ILogger<KafkaProducerManager>>();
        _producerManager = new KafkaProducerManager(producerOptions, producerLogger.Object);
    }

    /// <summary>
    /// Creates a <see cref="KafkaMaintenanceService"/> with the specified <see cref="KafkaOptions"/>.
    /// </summary>
    private KafkaMaintenanceService CreateService(KafkaOptions kafkaOptions)
    {
        var options = Options.Create(kafkaOptions);
        return new KafkaMaintenanceService(
            options,
            _mockAdministrationService.Object,
            _mockTopicValidator.Object,
            _producerManager,
            new SignatureService(new Mock<IEncryptionService>().Object),
            _mockConfiguration.Object,
            _logger);
    }

    /// <summary>
    /// Creates default <see cref="KafkaOptions"/> with a basic cluster for tests that need minimal setup.
    /// </summary>
    private static KafkaOptions CreateDefaultOptions()
    {
        return new KafkaOptions
        {
            Servers = "localhost:9092",
            DefaultClusterAlias = "Default",
            Clusters = new Dictionary<string, KafkaClusterConfiguration>
            {
                { "Default", new KafkaClusterConfiguration { BootstrapServers = "localhost:9092" } }
            }
        };
    }

    [Fact]
    public async Task RebuildIndexesAsync_EnabledOutboxWithoutConnectionName_ReturnsFailure()
    {
        var options = CreateDefaultOptions();
        options.Outbox.IsEnabled = true;
        options.Outbox.ConnectionStringName = string.Empty;
        var service = CreateService(options);

        var result = await service.RebuildIndexesAsync(CancellationToken.None);

        Assert.True(result.IsFailure);
        Assert.Equal("Kafka index maintenance database configuration is unavailable.", result.Error.Message);
    }

    // ──────────────────────────────────────────────────────────────────
    // ReplayDlqAsync
    // ──────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task ReplayDlqAsync_EmptyTopicConfigurationKey_ReturnsFailure(string? topicConfigurationKey)
    {
        // Arrange
        var service = CreateService(CreateDefaultOptions());

        // Act
        var result = await service.ReplayDlqAsync(topicConfigurationKey!, cancellationToken: CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Contains("must not be empty", result.Error.Message);
    }

    [Fact]
    public async Task ReplayDlqAsync_NonexistentTopicConfigurationKey_ReturnsFailure()
    {
        // Arrange
        var service = CreateService(CreateDefaultOptions());

        // Act
        var result = await service.ReplayDlqAsync("NonExistentTopic", cancellationToken: CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Contains("is not defined", result.Error.Message);
    }

    [Fact]
    public async Task ReplayDlqAsync_DlqDisabledForTopic_ReturnsFailure()
    {
        // Arrange
        var kafkaOptions = CreateDefaultOptions();
        kafkaOptions.Topics = new Dictionary<string, KafkaTopicConfiguration>
        {
            {
                "TestEvent", new KafkaTopicConfiguration
                {
                    TopicName = "test.events.created",
                    EnableDlq = false
                }
            }
        };
        var service = CreateService(kafkaOptions);

        // Act
        var result = await service.ReplayDlqAsync("TestEvent", cancellationToken: CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Contains("DLQ is not enabled", result.Error.Message);
    }

    // ──────────────────────────────────────────────────────────────────
    // PurgeStaleAssembliesAsync
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PurgeStaleAssembliesAsync_AssemblyNotEnabled_ReturnsFailure()
    {
        // Arrange
        var kafkaOptions = CreateDefaultOptions();
        kafkaOptions.Assembly = new MessageAssemblyOptions { IsEnabled = false };
        var service = CreateService(kafkaOptions);

        // Act
        var result = await service.PurgeStaleAssembliesAsync(CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Contains("assembly is not enabled", result.Error.Message);
    }

    [Fact]
    public async Task PurgeStaleAssembliesAsync_RedisProvider_ReturnsSuccessWithZeroCount()
    {
        // Arrange — Redis provider auto-expires via TTL, so the method returns 0 immediately.
        var kafkaOptions = CreateDefaultOptions();
        kafkaOptions.Assembly = new MessageAssemblyOptions
        {
            IsEnabled = true,
            Provider = AssemblyProvider.Redis
        };
        var service = CreateService(kafkaOptions);

        // Act
        var result = await service.PurgeStaleAssembliesAsync(CancellationToken.None);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(0, result.Value);
    }

    // ──────────────────────────────────────────────────────────────────
    // RetryDeadLetteredJobsAsync
    // ──────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task RetryDeadLetteredJobsAsync_EmptyJobType_ReturnsFailure(string? jobType)
    {
        // Arrange
        var service = CreateService(CreateDefaultOptions());

        // Act
        var result = await service.RetryDeadLetteredJobsAsync(jobType!, CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Contains("must not be empty", result.Error.Message);
    }

    [Fact]
    public async Task RetryDeadLetteredJobsAsync_StagedJobsNotEnabled_ReturnsFailure()
    {
        // Arrange
        var kafkaOptions = CreateDefaultOptions();
        kafkaOptions.StagedJobs = new StagedJobSettings { IsEnabled = false };
        var service = CreateService(kafkaOptions);

        // Act
        var result = await service.RetryDeadLetteredJobsAsync("ResolveWagerFact", CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Contains("Staged jobs engine is not enabled", result.Error.Message);
    }

    // ──────────────────────────────────────────────────────────────────
    // ValidateTopicAsync
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ValidateTopicAsync_DelegatesToTopicValidator()
    {
        // Arrange
        var expectedReport = new TopicValidationReport(
            TopicName: "test.events.created",
            TotalMessagesScanned: 50,
            ValidMessages: 48,
            InvalidMessages: 2,
            Errors: new List<ValidationError>());

        _mockTopicValidator
            .Setup(validator => validator.ValidateAsync("TestEvent", 50, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Success(expectedReport));

        var service = CreateService(CreateDefaultOptions());

        // Act
        var result = await service.ValidateTopicAsync("TestEvent", sampleSize: 50, CancellationToken.None);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(expectedReport, result.Value);
        _mockTopicValidator.Verify(
            validator => validator.ValidateAsync("TestEvent", 50, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    // ──────────────────────────────────────────────────────────────────
    // GetTopicMetadataAsync
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetTopicMetadataAsync_DelegatesToAdministrationService()
    {
        // Arrange
        var expectedMetadata = new TopicMetadata(
            TopicName: "test.events.created",
            PartitionCount: 6,
            ReplicationFactor: 3,
            Configuration: new Dictionary<string, string> { { "retention.ms", "604800000" } });

        _mockAdministrationService
            .Setup(service => service.GetTopicMetadataAsync("test.events.created", null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Success(expectedMetadata));

        var service = CreateService(CreateDefaultOptions());

        // Act
        var result = await service.GetTopicMetadataAsync("test.events.created", CancellationToken.None);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(expectedMetadata, result.Value);
        _mockAdministrationService.Verify(
            administration => administration.GetTopicMetadataAsync("test.events.created", null, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    // ──────────────────────────────────────────────────────────────────
    // GetPendingOutboxCountAsync
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetPendingOutboxCountAsync_OutboxNotEnabled_ReturnsFailure()
    {
        // Arrange
        var kafkaOptions = CreateDefaultOptions();
        kafkaOptions.Outbox = new KafkaOutboxSettings { IsEnabled = false };
        var service = CreateService(kafkaOptions);

        // Act
        var result = await service.GetPendingOutboxCountAsync(CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Contains("Outbox is not enabled", result.Error.Message);
    }
}
