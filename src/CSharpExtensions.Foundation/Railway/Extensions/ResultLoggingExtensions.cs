using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace CSharpExtensions.Foundation.Railway.Extensions;

/// <summary>
/// Logging extension methods for <see cref="Result"/> and <see cref="Result{TValue}"/> using the global logger.
/// </summary>
public static class ResultLoggingExtensions
{
    /// <summary>
    /// Logs the error if the result represents a failure, then returns the original result.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result LogIfFailure(this Result result)
    {
        if (result.IsFailure)
        {
            result.Error.Log(RailwayDiagnostics.Logger);
        }
        return result;
    }

    /// <summary>
    /// Logs a custom message along with the error details if the result represents a failure, then returns the original result.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result LogIfFailure(this Result result, string message, params object?[] args)
    {
        if (result.IsFailure)
        {
            RailwayDiagnostics.Logger.LogError(message, args);
            result.Error.Log(RailwayDiagnostics.Logger);
        }
        return result;
    }

    /// <summary>
    /// Logs the error if the result represents a failure, then returns the original result.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result<TValue> LogIfFailure<TValue>(this Result<TValue> result)
    {
        if (result.IsFailure)
        {
            result.Error.Log(RailwayDiagnostics.Logger);
        }
        return result;
    }

    /// <summary>
    /// Logs a custom message along with the error details if the result represents a failure, then returns the original result.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result<TValue> LogIfFailure<TValue>(this Result<TValue> result, string message, params object?[] args)
    {
        if (result.IsFailure)
        {
            RailwayDiagnostics.Logger.LogError(message, args);
            result.Error.Log(RailwayDiagnostics.Logger);
        }
        return result;
    }

    /// <summary>
    /// Asynchronously logs the error if the result represents a failure, then returns the original result.
    /// </summary>
    public static async Task<Result> LogIfFailureAsync(this Task<Result> resultTask)
    {
        var result = await resultTask;
        return result.LogIfFailure();
    }

    /// <summary>
    /// Asynchronously logs a custom message along with the error details if the result represents a failure, then returns the original result.
    /// </summary>
    public static async Task<Result> LogIfFailureAsync(this Task<Result> resultTask, string message, params object?[] args)
    {
        var result = await resultTask;
        return result.LogIfFailure(message, args);
    }

    /// <summary>
    /// Asynchronously logs the error if the result represents a failure, then returns the original result.
    /// </summary>
    public static async Task<Result<TValue>> LogIfFailureAsync<TValue>(this Task<Result<TValue>> resultTask)
    {
        var result = await resultTask;
        return result.LogIfFailure();
    }

    /// <summary>
    /// Asynchronously logs a custom message along with the error details if the result represents a failure, then returns the original result.
    /// </summary>
    public static async Task<Result<TValue>> LogIfFailureAsync<TValue>(this Task<Result<TValue>> resultTask, string message, params object?[] args)
    {
        var result = await resultTask;
        return result.LogIfFailure(message, args);
    }

    /// <summary>
    /// Logs a success message if the result represents a success, then returns the original result.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result LogIfSuccess(this Result result, string message, params object?[] args)
    {
        if (result.IsSuccess)
        {
            RailwayDiagnostics.Logger.LogInformation(message, args);
        }
        return result;
    }

    /// <summary>
    /// Logs a success message if the result represents a success, then returns the original result.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result<TValue> LogIfSuccess<TValue>(this Result<TValue> result, string message, params object?[] args)
    {
        if (result.IsSuccess)
        {
            RailwayDiagnostics.Logger.LogInformation(message, args);
        }
        return result;
    }

    /// <summary>
    /// Asynchronously logs a success message if the result represents a success, then returns the original result.
    /// </summary>
    public static async Task<Result> LogIfSuccessAsync(this Task<Result> resultTask, string message, params object?[] args)
    {
        var result = await resultTask;
        return result.LogIfSuccess(message, args);
    }

    /// <summary>
    /// Asynchronously logs a success message if the result represents a success, then returns the original result.
    /// </summary>
    public static async Task<Result<TValue>> LogIfSuccessAsync<TValue>(this Task<Result<TValue>> resultTask, string message, params object?[] args)
    {
        var result = await resultTask;
        return result.LogIfSuccess(message, args);
    }
}
