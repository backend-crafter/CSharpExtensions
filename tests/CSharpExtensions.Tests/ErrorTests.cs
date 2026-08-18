using CSharpExtensions.Core.Json.Extensions;
using CSharpExtensions.Core.Railway;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace CSharpExtensions.Tests;

public class ErrorTests
{
    [Fact]
    public void Constructor_ShouldSetDefaultValues()
    {
        var error = new Error("Test error");
        Assert.Equal("Test error", error.Message);
        Assert.Equal(500, error.HttpStatusCode);
        Assert.Equal("InternalServerError", error.Type);
        Assert.NotEqual(DateTime.MinValue, error.Timestamp);
    }

    [Fact]
    public void AsBadRequest_ShouldUpdateStatusAndType()
    {
        var error = new Error("Fail").AsBadRequest("ClientError", "Bad Request");
        Assert.Equal(400, error.HttpStatusCode);
        Assert.Equal("ClientError", error.Type);
        Assert.Equal("Bad Request", error.Title);
    }

    [Theory]
    [InlineData(399)]
    [InlineData(600)]
    public void AsHttpStatus_ShouldRejectNonErrorStatus(int statusCode)
    {
        var error = new Error("Fail");

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            error.AsHttpStatus(statusCode, "ClientError", "Request failed"));
    }

    [Fact]
    public void WithDetails_ShouldAddUniqueDetails()
    {
        var error = new Error("Base")
            .WithDetails("Detail 1")
            .WithDetails("Detail 2")
            .WithDetails("Detail 1"); // Duplicate

        Assert.Equal(2, error.Details.Count);
        Assert.Contains("Detail 1", error.Details);
        Assert.Contains("Detail 2", error.Details);
    }

    [Fact]
    public void WithMetadata_ShouldAddValues()
    {
        var error = new Error("Base")
            .WithMetadata("Key", "Value");

        Assert.Single(error.Metadata);
        Assert.Equal("Value", error.Metadata["Key"]);
    }

    [Fact]
    public void CausedBy_ShouldCaptureOnlyStableExceptionType()
    {
        Exception outer;
        try
        {
            try
            {
                throw new Exception("Inner exception");
            }
            catch (Exception inner)
            {
                throw new Exception("Outer exception", inner);
            }
        }
        catch (Exception ex)
        {
            outer = ex;
        }

        var error = new Error("Error").CausedBy(outer);

        Assert.Empty(error.Details);
        Assert.Empty(error.StackTraces);
        Assert.Equal(typeof(Exception).FullName, error.Metadata["exception_type"]);
        Assert.DoesNotContain("Outer exception", error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("Inner exception", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Equals_SameValues_ShouldBeTrue()
    {
        var error1 = new Error("Message").AsBadRequest("Type", "Title");
        var error2 = new Error("Message").AsBadRequest("Type", "Title");

        Assert.Equal(error1, error2);
    }

    [Fact]
    public void Equals_ShouldIgnoreMetadataInsertionOrder()
    {
        var first = new Error("Message")
            .WithMetadata("first", 1)
            .WithMetadata("second", 2);
        var second = new Error("Message")
            .WithMetadata("second", 2)
            .WithMetadata("first", 1);

        Assert.Equal(first, second);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
    }

    [Fact]
    public void Equals_ShouldNotChangeWhenLazyCollectionsAreRead()
    {
        var untouched = new Error("Message");
        var inspected = new Error("Message");

        Assert.Empty(inspected.Details);
        Assert.Empty(inspected.Metadata);

        Assert.Equal(untouched, inspected);
        Assert.Equal(untouched.GetHashCode(), inspected.GetHashCode());
    }

    [Fact]
    public void Log_ShouldInvokeLogger()
    {
        var loggerMock = new Mock<ILogger>();
        var error = new Error("credential-value");

        error.Log(loggerMock.Object);

        loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) =>
                    v.ToString()!.Contains("Operation failed", StringComparison.Ordinal) &&
                    !v.ToString()!.Contains("credential-value", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void LogBeforeReturn_ShouldInvokeLoggerAndReturnThis()
    {
        var loggerMock = new Mock<ILogger>();
        var error = new Error("credential-value");

        var returnedError = error.LogBeforeReturn(loggerMock.Object);

        Assert.Same(error, returnedError);
        loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) =>
                    v.ToString()!.Contains("Operation failed", StringComparison.Ordinal) &&
                    !v.ToString()!.Contains("credential-value", StringComparison.Ordinal)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void ToString_ShouldNotRenderDiagnosticPayloads()
    {
        var error = new Error("Token exchange failed")
            .AsBadRequest("Auth.InvalidCode", "Security Error")
            .WithDetails("The authorization code is invalid or has expired.")
            .WithMetadata("ClientId", "internal-client");

        var result = error.ToString();

        Assert.DoesNotContain("ClientId", result, StringComparison.Ordinal);
        Assert.DoesNotContain("internal-client", result, StringComparison.Ordinal);
        Assert.DoesNotContain("authorization code", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ToJson_ShouldNotSerializeInternalDiagnosticCollections()
    {
        var error = new Error("Token exchange failed")
            .AsBadRequest("Auth.InvalidCode", "Security Error")
            .WithDetails("The authorization code is invalid or has expired.")
            .WithMetadata("ClientId", "internal-client");

        var json = error.ToJson();

        Assert.DoesNotContain("metadata", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("details", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("internal-client", json, StringComparison.Ordinal);
        Assert.DoesNotContain("authorization code", json, StringComparison.OrdinalIgnoreCase);
    }
}
