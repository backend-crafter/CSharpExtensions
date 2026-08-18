using System;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Core.Helpers.Extensions;
using CSharpExtensions.Core.Railway;
using CSharpExtensions.Core.Railway.Extensions;
using Xunit;

namespace CSharpExtensions.Tests;

public class ResilienceTests
{
    [Fact]
    public async Task TryAgainAsync_Standard_ShouldReturnValue_OnFirstAttemptSuccess()
    {
        // Arrange
        var attempts = 0;
        Func<Task<int>> action = () =>
        {
            attempts++;
            return Task.FromResult(42);
        };

        // Act
        var result = await action.TryAgainAsync(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero);

        // Assert
        Assert.Equal(42, result);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task TryAgainAsync_Standard_ShouldRetryAndSucceed()
    {
        // Arrange
        var attempts = 0;
        Func<Task<int>> action = () =>
        {
            attempts++;
            if (attempts < 3)
            {
                throw new InvalidOperationException("Transient error");
            }
            return Task.FromResult(100);
        };

        // Act
        var result = await action.TryAgainAsync(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero,
            shouldRetry: exception => exception is InvalidOperationException);

        // Assert
        Assert.Equal(100, result);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task TryAgainAsync_Standard_ShouldThrow_WhenMaxAttemptsReached()
    {
        // Arrange
        var attempts = 0;
        Func<Task<int>> action = () =>
        {
            attempts++;
            throw new InvalidOperationException("Persistent error");
        };

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            action.TryAgainAsync(
                maxAttempts: 3,
                initialDelay: TimeSpan.Zero,
                shouldRetry: exception => exception is InvalidOperationException));

        Assert.Equal("Persistent error", exception.Message);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task TryAgainAsync_Standard_ShouldNotRetryWithoutPredicate()
    {
        var attempts = 0;
        Func<Task<int>> action = () =>
        {
            attempts++;
            throw new InvalidOperationException("Permanent error");
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            action.TryAgainAsync(
                maxAttempts: 3,
                initialDelay: TimeSpan.Zero));

        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task TryAgainAsync_Standard_ShouldNotRetry_WhenShouldRetryReturnsFalse()
    {
        // Arrange
        var attempts = 0;
        Func<Task<int>> action = () =>
        {
            attempts++;
            throw new ArgumentException("Fatal error");
        };

        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            action.TryAgainAsync(
                maxAttempts: 5,
                initialDelay: TimeSpan.Zero,
                shouldRetry: ex => ex is not ArgumentException));

        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task TryAgainAsync_Standard_ShouldCancel_WhenCancellationTokenSignaled()
    {
        // Arrange
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        Func<Task<int>> action = () => Task.FromResult(42);

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            action.TryAgainAsync(
                maxAttempts: 3,
                initialDelay: TimeSpan.Zero,
                cancellationToken: cancellationTokenSource.Token));
    }

    [Fact]
    public async Task TryAgainAsync_Rop_ShouldReturnValue_OnFirstAttemptSuccess()
    {
        // Arrange
        var attempts = 0;
        Func<Task<Result<int>>> action = () =>
        {
            attempts++;
            return Task.FromResult(Result.Success(42));
        };

        // Act
        var result = await action.TryResultAgainAsync(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(42, result.Value);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task TryAgainAsync_Rop_ShouldRetryAndSucceed()
    {
        // Arrange
        var attempts = 0;
        Func<Task<Result<int>>> action = () =>
        {
            attempts++;
            if (attempts < 3)
            {
                return Task.FromResult(Result.Failure<int>("Transient error"));
            }
            return Task.FromResult(Result.Success(200));
        };

        // Act
        var result = await action.TryResultAgainAsync(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero,
            shouldRetry: _ => true);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(200, result.Value);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task TryAgainAsync_Rop_ShouldReturnFailure_WhenMaxAttemptsReached()
    {
        // Arrange
        var attempts = 0;
        Func<Task<Result<int>>> action = () =>
        {
            attempts++;
            return Task.FromResult(Result.Failure<int>("Persistent error"));
        };

        // Act
        var result = await action.TryResultAgainAsync(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero,
            shouldRetry: _ => true);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Equal("Persistent error", result.Error.Message);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task TryAgainAsync_Rop_ShouldNotRetryWithoutPredicate()
    {
        var attempts = 0;
        Func<Task<Result<int>>> action = () =>
        {
            attempts++;
            return Task.FromResult(Result.Failure<int>("Permanent error"));
        };

        var result = await action.TryResultAgainAsync(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero);

        Assert.True(result.IsFailure);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task TryAgainAsync_Rop_ShouldNotRetry_WhenShouldRetryReturnsFalse()
    {
        // Arrange
        var attempts = 0;
        Func<Task<Result<int>>> action = () =>
        {
            attempts++;
            return Task.FromResult(Result.Failure<int>("Fatal error"));
        };

        // Act
        var result = await action.TryResultAgainAsync(
            maxAttempts: 5,
            initialDelay: TimeSpan.Zero,
            shouldRetry: error => error.Message != "Fatal error");

        // Assert
        Assert.True(result.IsFailure);
        Assert.Equal("Fatal error", result.Error.Message);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task TryAgainAsync_Rop_ShouldRetryOnException_WhenShouldRetryExceptionReturnsTrue()
    {
        // Arrange
        var attempts = 0;
        Func<Task<Result<int>>> action = () =>
        {
            attempts++;
            if (attempts < 3)
            {
                throw new InvalidOperationException("Technical issue");
            }
            return Task.FromResult(Result.Success(300));
        };

        // Act
        var result = await action.TryResultAgainAsync(
            maxAttempts: 3,
            initialDelay: TimeSpan.Zero,
            shouldRetryException: ex => ex is InvalidOperationException);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(300, result.Value);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task TryAgainAsync_Rop_ShouldThrowException_WhenShouldRetryExceptionReturnsFalse()
    {
        // Arrange
        var attempts = 0;
        Func<Task<Result<int>>> action = () =>
        {
            attempts++;
            throw new ArgumentException("Fatal bug");
        };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() =>
            action.TryResultAgainAsync(
                maxAttempts: 3,
                initialDelay: TimeSpan.Zero,
                shouldRetryException: ex => ex is InvalidOperationException));

        Assert.Equal(1, attempts);
    }
}
