using CSharpExtensions.Core.Railway;
using Xunit;

namespace CSharpExtensions.Tests;

public class ResultTTests
{
    [Fact]
    public void Success_ShouldHaveValueAndBeSuccess()
    {
        var result = Result.Success("Data");
        Assert.True(result.IsSuccess);
        Assert.Equal("Data", result.Value);
    }

    [Fact]
    public void Value_AccessingFailure_ShouldThrowException()
    {
        var result = Result.Failure<string>("Error");
        Assert.Throws<InvalidOperationException>(() => result.Value);
    }

    [Fact]
    public void ValueOrDefault_AccessingFailure_ShouldReturnDefault()
    {
        var result = Result.Failure<string>("Error");
        Assert.Null(result.ValueOrDefault);
        
        var intResult = Result.Failure<int>("Error");
        Assert.Equal(0, intResult.ValueOrDefault);
    }

    [Fact]
    public void ImplicitConversion_FromError_ShouldBeFailure()
    {
        Error error = new Error("Failed");
        Result<int> result = error;

        Assert.True(result.IsFailure);
        Assert.Equal(error, result.Error);
    }

    [Fact]
    public void ImplicitConversion_FromValue_ShouldBeSuccess()
    {
        Result<int> result = 42;

        Assert.True(result.IsSuccess);
        Assert.Equal(42, result.Value);
    }
}
