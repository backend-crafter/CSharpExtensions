namespace CSharpExtensions.Tests.Kafka;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

public sealed class KafkaMaintenanceHostedServiceTests
{
    private readonly Mock<IRedisConnectionResolver> _mockRedisResolver;

    public KafkaMaintenanceHostedServiceTests()
    {
        _mockRedisResolver = new Mock<IRedisConnectionResolver>();
    }

    [Fact]
    public void SqlLockAcquireCommand_CapturesStoredProcedureReturnValue()
    {
        Assert.Contains(
            "EXEC @LockResult = sys.sp_getapplock",
            KafkaMaintenanceHostedService.SqlLockAcquireCommand,
            StringComparison.Ordinal);
        Assert.Contains(
            "SELECT @LockResult;",
            KafkaMaintenanceHostedService.SqlLockAcquireCommand,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(1, true)]
    [InlineData(-1, false)]
    [InlineData(-2, false)]
    [InlineData(-3, false)]
    [InlineData(-999, false)]
    public void IsSqlLockAcquired_RecognizesSqlServerReturnCodes(int result, bool expected)
    {
        Assert.Equal(expected, KafkaMaintenanceHostedService.IsSqlLockAcquired(result));
    }

    [Fact]
    public async Task StartAsync_And_StopAsync_ExecuteWithoutThrowingExceptions()
    {
        // Arrange
        var options = new KafkaOptions
        {
            Maintenance = new KafkaMaintenanceSettings
            {
                IntervalMinutes = 60
            },
            Outbox = new KafkaOutboxSettings
            {
                ConnectionStringName = "DefaultConnection"
            }
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                { "ConnectionStrings:DefaultConnection", "Server=localhost;Database=test;Trusted_Connection=True;" }
            })
            .Build();

        var service = new KafkaMaintenanceHostedService(
            _mockRedisResolver.Object,
            Options.Create(options),
            configuration,
            NullLogger<KafkaMaintenanceHostedService>.Instance);

        using var cancellationTokenSource = new CancellationTokenSource();

        // Act
        // Verify we can start the service (triggers ExecuteAsync in the background)
        await service.StartAsync(cancellationTokenSource.Token);

        // Verify we can stop it immediately
        await service.StopAsync(CancellationToken.None);

        // Assert
        // If we reached here without exceptions, the test passes
        Assert.True(true);
    }
}
