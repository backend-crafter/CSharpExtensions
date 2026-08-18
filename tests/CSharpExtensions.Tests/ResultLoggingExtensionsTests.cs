using CSharpExtensions.Foundation.Railway;
using CSharpExtensions.Foundation.Railway.Extensions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace CSharpExtensions.Tests;

/// <summary>
/// Unit tests for ResultLoggingExtensions verifying implicit global logging behavior.
/// </summary>
public class ResultLoggingExtensionsTests
{
    private readonly Mock<ILoggerFactory> _loggerFactoryMock = new();
    private readonly Mock<ILogger> _loggerMock = new();

    public ResultLoggingExtensionsTests()
    {
        _loggerFactoryMock
            .Setup(f => f.CreateLogger(It.IsAny<string>()))
            .Returns(_loggerMock.Object);

        RailwayDiagnostics.Configure(_loggerFactoryMock.Object);
    }

    [Fact]
    public void LogIfFailure_ShouldLog_WhenResultIsFailure()
    {
        // Arrange
        var result = Result.Failure("Test error");

        // Act
        var returnedResult = result.LogIfFailure();

        // Assert
        Assert.True(returnedResult.IsFailure);
        var message = Assert.Single(_loggerMock.Invocations).Arguments[2]?.ToString() ?? string.Empty;
        Assert.Contains("ErrorType=InternalServerError", message);
        Assert.DoesNotContain("Test error", message);
    }

    [Fact]
    public void LogIfFailure_ShouldNotLog_WhenResultIsSuccess()
    {
        // Arrange
        var result = Result.Success();

        // Act
        var returnedResult = result.LogIfFailure();

        // Assert
        Assert.True(returnedResult.IsSuccess);
        _loggerMock.Verify(
            x => x.Log(
                It.IsAny<LogLevel>(),
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Never);
    }

    [Fact]
    public void LogIfFailure_WithMessage_ShouldLogCustomMessage_WhenResultIsFailure()
    {
        // Arrange
        var result = Result.Failure("Inner error detail");

        // Act
        var returnedResult = result.LogIfFailure("Operation failed for UserId={UserId}", "12345");

        // Assert
        Assert.True(returnedResult.IsFailure);
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Operation failed for UserId=12345")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task LogIfFailureAsync_ShouldLog_WhenTaskResultIsFailure()
    {
        // Arrange
        var task = Task.FromResult(Result.Failure("Async error"));

        // Act
        var returnedResult = await task.LogIfFailureAsync("Async operation failed");

        // Assert
        Assert.True(returnedResult.IsFailure);
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Async operation failed")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void Error_LogBeforeReturn_ShouldLogUsingGlobalLoggerAndReturnThis()
    {
        // Arrange
        RailwayDiagnostics.Configure(_loggerFactoryMock.Object);
        _loggerMock.Invocations.Clear();
        var error = new Error("Fluent error log message");

        // Act
        var returnedError = error.LogBeforeReturn();

        // Assert
        Assert.Same(error, returnedError);
        var message = Assert.Single(_loggerMock.Invocations).Arguments[2]?.ToString() ?? string.Empty;
        Assert.Contains("ErrorType=InternalServerError", message);
        Assert.DoesNotContain("Fluent error log message", message);
    }

    [Fact]
    public void LogIfSuccess_ShouldLog_WhenResultIsSuccess()
    {
        // Arrange
        var result = Result.Success();

        // Act
        var returnedResult = result.LogIfSuccess("Operation completed successfully for {Entity}", "VIP");

        // Assert
        Assert.True(returnedResult.IsSuccess);
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Operation completed successfully for VIP")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task LogIfSuccessAsync_ShouldNotLog_WhenTaskResultIsFailure()
    {
        // Arrange
        var task = Task.FromResult(Result.Failure("Failed result"));

        // Act
        var returnedResult = await task.LogIfSuccessAsync("Operation completed successfully");

        // Assert
        Assert.True(returnedResult.IsFailure);
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Never);
    }
}
