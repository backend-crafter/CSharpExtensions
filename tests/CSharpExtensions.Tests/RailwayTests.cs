using CSharpExtensions.Core.Railway;
using CSharpExtensions.Core.Railway.Extensions;
using Xunit;

#pragma warning disable CS0618

namespace CSharpExtensions.Tests;

public class RailwayTests
{
    [Fact]
    public void Success_ShouldReturnSuccessState()
    {
        var result = Result.Success();
        Assert.True(result.IsSuccess);
        Assert.False(result.IsFailure);
        Assert.Equal(Error.None, result.Error);
    }

    [Fact]
    public void SuccessWithValue_ShouldReturnValue()
    {
        var result = Result.Success(42);
        Assert.True(result.IsSuccess);
        Assert.Equal(42, result.Value);
    }

    [Fact]
    public void Failure_ShouldReturnFailureState()
    {
        var error = new Error("Something went wrong");
        var result = Result.Failure(error);
        Assert.True(result.IsFailure);
        Assert.False(result.IsSuccess);
        Assert.Equal(error, result.Error);
    }

    [Fact]
    public void FailureWithMessage_ShouldCreateErrorWithMessage()
    {
        var result = Result.Failure("Error message");
        Assert.True(result.IsFailure);
        Assert.Equal("Error message", result.Error.Message);
    }

    [Fact]
    public void Create_WithValue_ShouldReturnSuccess()
    {
        var result = Result.Create("value");
        Assert.True(result.IsSuccess);
        Assert.Equal("value", result.Value);
    }

    [Fact]
    public void Create_WithNull_ShouldReturnFailure()
    {
        var result = Result.Create<string>(null);
        Assert.True(result.IsFailure);
        Assert.Contains("Expected value of type String was null.", result.Error.Message);
    }

    [Fact]
    public void Transform_ShouldMapValueWhenSuccess()
    {
        var result = Result.Success(10)
            .Transform(v => v * 2);

        Assert.True(result.IsSuccess);
        Assert.Equal(20, result.Value);
    }

    [Fact]
    public void Transform_ShouldStayFailureWhenFailure()
    {
        var result = Result.Failure<int>("Fail")
            .Transform(v => v * 2);

        Assert.True(result.IsFailure);
        Assert.Equal("Fail", result.Error.Message);
    }

    [Fact]
    public void Then_ShouldChainMethodsWhenSuccess()
    {
        var result = Result.Success(10)
            .Then(v => Result.Success(v.ToString()));

        Assert.True(result.IsSuccess);
        Assert.Equal("10", result.Value);
    }

    [Fact]
    public void Then_ShouldSwitchToFailureWhenThenMethodFails()
    {
        var result = Result.Success(10)
            .Then(v => Result.Failure<string>("Then failed"));

        Assert.True(result.IsFailure);
        Assert.Equal("Then failed", result.Error.Message);
    }

    [Fact]
    public void Ensure_ShouldFailIfPredicateIsFalse()
    {
        var result = Result.Success(10)
            .Ensure(v => v > 20, new Error("Too small"));

        Assert.True(result.IsFailure);
        Assert.Equal("Too small", result.Error.Message);
    }

    [Fact]
    public void Match_ShouldExecuteCorrectAction()
    {
        var successPath = Result.Success(10).Match(v => "Success", e => "Failure");
        var failurePath = Result.Failure<int>("Error").Match(v => "Success", e => "Failure");

        Assert.Equal("Success", successPath);
        Assert.Equal("Failure", failurePath);
    }

    [Fact]
    public void OnFailure_ShouldExecuteAction_WhenFailure()
    {
        var called = false;
        var result = Result.Failure("Error");
        
        result.OnFailure(e => called = true);
        
        Assert.True(called);
    }

    [Fact]
    public async Task OnFailureAsync_ShouldExecuteAction_WhenFailure()
    {
        var called = false;
        var result = Result.Failure("Error");
        
        await result.OnFailureAsync(async e => {
            await Task.Yield();
            called = true;
        });
        
        Assert.True(called);
    }

    [Fact]
    public void OnFailure_ShouldNotExecuteAction_WhenSuccess()
    {
        var called = false;
        var result = Result.Success();
        
        result.OnFailure(e => called = true);
        
        Assert.False(called);
    }

    [Fact]
    public void Then_SyncResultToResult_ShouldChainWhenSuccess()
    {
        var result = Result.Success()
            .Then(() => Result.Success());

        Assert.True(result.IsSuccess);
    }

    [Fact]
    public void Then_SyncGenericToGeneric_ShouldChainWhenSuccess()
    {
        var result = Result.Success(10)
            .Then(v => Result.Success(v.ToString()));

        Assert.True(result.IsSuccess);
        Assert.Equal("10", result.Value);
    }

    [Fact]
    public void Then_SyncGenericToResult_ShouldChainWhenSuccess()
    {
        var result = Result.Success(10)
            .Then(v => Result.Success());

        Assert.True(result.IsSuccess);
    }

    [Fact]
    public async Task ThenAsync_AsyncResultToTaskResult_ShouldChainWhenSuccess()
    {
        var result = await Result.Success()
            .ThenAsync(async () => {
                await Task.Yield();
                return Result.Success();
            });

        Assert.True(result.IsSuccess);
    }

    [Fact]
    public async Task ThenAsync_AsyncGenericToTaskGeneric_ShouldChainWhenSuccess()
    {
        var result = await Result.Success(10)
            .ThenAsync(async v => {
                await Task.Yield();
                return Result.Success(v * 2);
            });

        Assert.True(result.IsSuccess);
        Assert.Equal(20, result.Value);
    }

    [Fact]
    public async Task ThenAsync_DeepChaining_ShouldPropagateValuesAndFailures()
    {
        var result = await Task.FromResult(Result.Success(5))
            .ThenAsync(async v => {
                await Task.Yield();
                return Result.Success(v + 5);
            })
            .ThenAsync(v => Result.Success(v * 2))
            .ThenAsync(async v => {
                await Task.Yield();
                return Result.Success(v.ToString());
            });

        Assert.True(result.IsSuccess);
        Assert.Equal("20", result.Value);
    }

    [Fact]
    public async Task ThenAsync_DeepChaining_ShouldAbortOnFailure()
    {
        var callCounter = 0;
        var result = await Task.FromResult(Result.Success(5))
            .ThenAsync(v => Result.Failure<int>("First failure"))
            .ThenAsync(async v => {
                callCounter++;
                await Task.Yield();
                return Result.Success(v * 2);
            });

        Assert.True(result.IsFailure);
        Assert.Equal("First failure", result.Error.Message);
        Assert.Equal(0, callCounter);
    }
}
