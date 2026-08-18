namespace CSharpExtensions.Tests.Kafka;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using Xunit;

public class RedisDistributedDuplicateDetectorTests
{
    private readonly Mock<IRedisConnectionResolver> _mockConnectionResolver;
    private readonly Mock<IConnectionMultiplexer> _mockMultiplexer;
    private readonly Mock<IDatabase> _mockDatabase;
    private readonly KafkaOptions _options;
    private readonly RedisDistributedDuplicateDetector _detector;

    public RedisDistributedDuplicateDetectorTests()
    {
        _mockConnectionResolver = new Mock<IRedisConnectionResolver>();
        _mockMultiplexer = new Mock<IConnectionMultiplexer>();
        _mockDatabase = new Mock<IDatabase>();

        _options = new KafkaOptions
        {
            Idempotency = new IdempotencyOptions
            {
                RedisConnectionAlias = "Idempotency"
            }
        };

        _mockConnectionResolver.Setup(r => r.IsRegistered("Idempotency")).Returns(true);
        _mockConnectionResolver.Setup(r => r.Resolve("Idempotency")).Returns(_mockMultiplexer.Object);
        _mockMultiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(_mockDatabase.Object);

        _detector = new RedisDistributedDuplicateDetector(
            _mockConnectionResolver.Object,
            Options.Create(_options));
    }

    [Fact]
    public async Task TryClaimUniqueAsync_WhenConnectionNotRegistered_FailsClosed()
    {
        // Arrange
        _mockConnectionResolver.Setup(r => r.IsRegistered("Idempotency")).Returns(false);

        // Act
        var result = await _detector.TryClaimUniqueAsync("msg1", "group1", 60);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Equal("Kafka.IdempotencyUnavailable", result.Error.Type);
    }

    [Fact]
    public async Task TryClaimUniqueAsync_WhenLuaReturns1_ReturnsSuccessTrue()
    {
        // Arrange
        _mockDatabase.Setup(d => d.ScriptEvaluateAsync(
            It.IsAny<string>(),
            It.IsAny<RedisKey[]>(),
            It.IsAny<RedisValue[]>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisResult.Create(1));

        // Act
        var result = await _detector.TryClaimUniqueAsync("msg1", "group1", 60, ownerToken: "owner1");

        // Assert
        Assert.True(result.IsSuccess);
        Assert.True(result.Value);
    }

    [Fact]
    public async Task TryClaimUniqueAsync_WhenLuaReturns0_ReturnsSuccessFalse()
    {
        // Arrange
        _mockDatabase.Setup(d => d.ScriptEvaluateAsync(
            It.IsAny<string>(),
            It.IsAny<RedisKey[]>(),
            It.IsAny<RedisValue[]>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisResult.Create(0));

        // Act
        var result = await _detector.TryClaimUniqueAsync("msg1", "group1", 60, ownerToken: "owner1");

        // Assert
        Assert.True(result.IsSuccess);
        Assert.False(result.Value);
    }

    [Fact]
    public async Task TryClaimUniqueAsync_WhenAnotherOwnerIsProcessing_FailsWithoutDuplicateResult()
    {
        _mockDatabase.Setup(d => d.ScriptEvaluateAsync(
            It.IsAny<string>(),
            It.IsAny<RedisKey[]>(),
            It.IsAny<RedisValue[]>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisResult.Create(2));

        var result = await _detector.TryClaimUniqueAsync(
            "msg1",
            "group1",
            60,
            ownerToken: "owner2");

        Assert.True(result.IsFailure);
        Assert.Equal(RedisDistributedDuplicateDetector.InFlightErrorType, result.Error.Type);
    }

    [Fact]
    public async Task TryClaimUniqueAsync_UsesLeaseLongerThanConsumerMaxPollInterval()
    {
        RedisValue[]? capturedValues = null;
        _mockDatabase.Setup(d => d.ScriptEvaluateAsync(
            It.IsAny<string>(),
            It.IsAny<RedisKey[]>(),
            It.IsAny<RedisValue[]>(),
            It.IsAny<CommandFlags>()))
            .Callback<string, RedisKey[], RedisValue[], CommandFlags>((_, _, values, _) =>
            {
                capturedValues = values;
            })
            .ReturnsAsync(RedisResult.Create(1));

        var result = await _detector.TryClaimUniqueAsync(
            "msg1",
            "group1",
            60,
            ownerToken: "owner1");

        Assert.True(result.IsSuccess);
        Assert.NotNull(capturedValues);
        Assert.True((int)capturedValues[0] > _options.Consumer.MaxPollIntervalMs / 1000);
    }

    [Fact]
    public async Task CompleteClaimAsync_WhenLuaReturns1_ReturnsSuccess()
    {
        // Arrange
        _mockDatabase.Setup(d => d.ScriptEvaluateAsync(
            It.IsAny<string>(),
            It.IsAny<RedisKey[]>(),
            It.IsAny<RedisValue[]>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisResult.Create(1));

        // Act
        var result = await _detector.CompleteClaimAsync("msg1", "group1", ownerToken: "owner1");

        // Assert
        Assert.True(result.IsSuccess);
    }

    [Fact]
    public async Task CompleteClaimAsync_WhenLuaReturns0_ReturnsFailure()
    {
        // Arrange
        _mockDatabase.Setup(d => d.ScriptEvaluateAsync(
            It.IsAny<string>(),
            It.IsAny<RedisKey[]>(),
            It.IsAny<RedisValue[]>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisResult.Create(0));

        // Act
        var result = await _detector.CompleteClaimAsync("msg1", "group1", ownerToken: "owner1");

        // Assert
        Assert.True(result.IsFailure);
        Assert.Contains("Failed to complete claim. Lease lost or owned by another instance", result.Error.Message);
    }

    [Fact]
    public async Task LegacyClaimWithoutOwner_CanBeCompletedWithStableOwnerToken()
    {
        var capturedOwners = new List<string>();
        _mockDatabase.Setup(d => d.ScriptEvaluateAsync(
            It.IsAny<string>(),
            It.IsAny<RedisKey[]>(),
            It.IsAny<RedisValue[]>(),
            It.IsAny<CommandFlags>()))
            .Callback<string, RedisKey[], RedisValue[], CommandFlags>((_, _, values, _) =>
                capturedOwners.Add(values[1].ToString()))
            .ReturnsAsync(RedisResult.Create(1));

        var claim = await _detector.TryClaimUniqueAsync("msg1", "group1", 60);
        var complete = await _detector.CompleteClaimAsync("msg1", "group1");

        Assert.True(claim.IsSuccess);
        Assert.True(complete.IsSuccess);
        Assert.Equal([RedisDistributedDuplicateDetector.LegacyOwnerToken, RedisDistributedDuplicateDetector.LegacyOwnerToken], capturedOwners);
    }

    [Fact]
    public async Task TryClaimUniqueAsync_WithRedisKeyDelimiterInIdentifier_FailsBeforeRedis()
    {
        var result = await _detector.TryClaimUniqueAsync("msg:1", "group1", 60);

        Assert.True(result.IsFailure);
        Assert.Equal("Kafka.IdempotencyInvalidInput", result.Error.Type);
        _mockDatabase.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task RenewClaimAsync_ExtendsOnlyOwnerFencedClaim()
    {
        _mockDatabase.Setup(d => d.ScriptEvaluateAsync(
            It.IsAny<string>(),
            It.IsAny<RedisKey[]>(),
            It.IsAny<RedisValue[]>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisResult.Create(1));

        var result = await _detector.RenewClaimAsync("msg1", "group1", "owner1");

        Assert.True(result.IsSuccess);
        _mockDatabase.Verify(d => d.ScriptEvaluateAsync(
            It.Is<string>(script => script.Contains("EXPIRE") && script.Contains("Processing:")),
            It.Is<RedisKey[]>(keys => keys[0] == "idempotency:group1:msg1"),
            It.Is<RedisValue[]>(values => values[0] == "owner1"),
            It.IsAny<CommandFlags>()), Times.Once);
    }

    [Fact]
    public async Task RenewClaimAsync_WhenOwnerLost_ReturnsFailure()
    {
        _mockDatabase.Setup(d => d.ScriptEvaluateAsync(
            It.IsAny<string>(),
            It.IsAny<RedisKey[]>(),
            It.IsAny<RedisValue[]>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisResult.Create(0));

        var result = await _detector.RenewClaimAsync("msg1", "group1", "stale-owner");

        Assert.True(result.IsFailure);
        Assert.Contains("Lease lost", result.Error.Message);
    }

    [Fact]
    public async Task ReleaseClaimAsync_EvaluatesCorrectScript()
    {
        // Arrange
        _mockDatabase.Setup(d => d.ScriptEvaluateAsync(
            It.IsAny<string>(),
            It.IsAny<RedisKey[]>(),
            It.IsAny<RedisValue[]>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisResult.Create(1));

        // Act
        var result = await _detector.ReleaseClaimAsync("msg1", "group1", ownerToken: "owner1");

        // Assert
        Assert.True(result.IsSuccess);
        _mockDatabase.Verify(d => d.ScriptEvaluateAsync(
            It.Is<string>(script => script.Contains("DEL")),
            It.Is<RedisKey[]>(keys => keys[0] == "idempotency:group1:msg1"),
            It.Is<RedisValue[]>(values => values[0] == "owner1"),
            It.IsAny<CommandFlags>()), Times.Once);
    }
}
