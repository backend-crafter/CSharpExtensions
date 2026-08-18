using CSharpExtensions.Foundation.Security.Interfaces;

namespace CSharpExtensions.Tests.Kafka;

using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

public sealed class KafkaOutboxProcessorTests
{
    [Fact]
    public void SqlProvisioningLockAcquireCommand_CapturesStoredProcedureReturnValue()
    {
        Assert.Contains(
            "EXEC @LockResult = sys.sp_getapplock",
            KafkaOutboxProcessor.SqlProvisioningLockAcquireCommand,
            StringComparison.Ordinal);
        Assert.Contains(
            "SELECT @LockResult;",
            KafkaOutboxProcessor.SqlProvisioningLockAcquireCommand,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(1, true)]
    [InlineData(-1, false)]
    [InlineData(-2, false)]
    [InlineData(-3, false)]
    [InlineData(-999, false)]
    public void IsProvisioningLockAcquired_RecognizesSqlServerReturnCodes(int result, bool expected)
    {
        Assert.Equal(expected, KafkaOutboxProcessor.IsProvisioningLockAcquired(result));
    }

    [Theory]
    [InlineData(500, 0, 500)]
    [InlineData(500, 2, 2500)]
    [InlineData(500, 5, 30000)]
    [InlineData(5000, 5, 60000)]
    public void CalculateEmptyBatchDelayMs_UsesConfiguredBaseAndCap(
        int baseDelayMs,
        int tierIndex,
        int expected)
    {
        Assert.Equal(expected, KafkaOutboxProcessor.CalculateEmptyBatchDelayMs(baseDelayMs, tierIndex));
    }

    [Theory]
    [InlineData(3000, 1, 3000)]
    [InlineData(3000, 2, 6000)]
    [InlineData(3000, 6, 60000)]
    public void CalculateErrorDelayMs_UsesConfiguredBaseAndCap(
        int baseDelayMs,
        int consecutiveErrorCount,
        int expected)
    {
        Assert.Equal(
            expected,
            KafkaOutboxProcessor.CalculateErrorDelayMs(baseDelayMs, consecutiveErrorCount));
    }

    [Theory]
    [InlineData(5, 300, 1, 5)]
    [InlineData(5, 300, 3, 20)]
    [InlineData(5, 300, 10, 300)]
    public void CalculateRetryDelaySeconds_UsesExponentialScheduleAndCap(
        int baseDelaySeconds,
        int maxDelaySeconds,
        int attemptCount,
        int expected)
    {
        Assert.Equal(
            expected,
            KafkaOutboxProcessor.CalculateRetryDelaySeconds(baseDelaySeconds, maxDelaySeconds, attemptCount));
    }

    [Fact]
    public void AddKafka_RegistersSelfDisablingOutboxWorkerForJsonActivation()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Servers"] = "localhost:9092"
            })
            .Build();

        services.AddKafka(configuration);

        Assert.Contains(
            services,
            descriptor => descriptor.ServiceType == typeof(IHostedService)
                && descriptor.ImplementationType == typeof(KafkaOutboxProcessor));
    }

    [Fact]
    public void AddKafka_WithoutAuthenticatedTopics_DoesNotRequireEncryptionService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Servers"] = "localhost:9092"
            })
            .Build();
        services.AddSingleton<IConfiguration>(configuration);

        services.AddKafka(configuration);

        using var serviceProvider = services.BuildServiceProvider();
        Assert.Null(serviceProvider.GetService<IEncryptionService>());
        Assert.NotNull(serviceProvider.GetRequiredService<SignatureService>());
    }

    [Theory]
    [InlineData("MissingConnection")]
    [InlineData("Primary,Secondary")]
    public async Task StartAsync_InvalidOrUnavailableConnection_FailsStartup(string connectionName)
    {
        var options = new KafkaOptions();
        options.Outbox.IsEnabled = true;
        options.Outbox.ConnectionStringName = connectionName;
        using var producerManager = new KafkaProducerManager(
            Options.Create(options),
            Mock.Of<ILogger<KafkaProducerManager>>());
        using var processor = new KafkaOutboxProcessor(
            new ConfigurationBuilder().Build(),
            producerManager,
            new SignatureService(Mock.Of<IEncryptionService>()),
            new S3ClaimCheckOffloader(),
            Options.Create(options),
            Mock.Of<ILogger<KafkaOutboxProcessor>>());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => processor.StartAsync(CancellationToken.None));

        Assert.Contains("connection string", exception.Message, StringComparison.OrdinalIgnoreCase);
    }
}
