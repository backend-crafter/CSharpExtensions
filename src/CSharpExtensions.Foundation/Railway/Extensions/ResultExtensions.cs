using System.Runtime.CompilerServices;

namespace CSharpExtensions.Foundation.Railway.Extensions;

/// <summary>
/// Functional extensions for the <see cref="Result"/> and <see cref="Result{TValue}"/> types.
/// Supports the Railway Oriented Programming pattern with semantic clarity.
/// </summary>
public static class ResultExtensions
{
    #region Sync Transformations (Transform / Bind / Then)

    /// <summary>
    /// Transforms the value of a successful result using a synchronous function.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result<TOut> Transform<TIn, TOut>(this Result<TIn> result, Func<TIn, TOut> func)
    {
        return result.IsSuccess ? Result.Success(func(result.Value)) : Result.Failure<TOut>(result.Error);
    }


    /// <summary>
    /// Chains a synchronous operation that returns a <see cref="Result"/> if the current result is successful.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result Then(this Result result, Func<Result> func)
    {
        return result.IsSuccess ? func() : result;
    }

    /// <summary>
    /// Chains a synchronous operation that returns a <see cref="Result{TOut}"/> if the current result is successful.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result<TOut> Then<TIn, TOut>(this Result<TIn> result, Func<TIn, Result<TOut>> func)
    {
        return result.IsSuccess ? func(result.Value) : Result.Failure<TOut>(result.Error);
    }

    /// <summary>
    /// Chains a synchronous operation that returns a <see cref="Result"/> if the current result is successful.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result Then<TIn>(this Result<TIn> result, Func<TIn, Result> func)
    {
        return result.IsSuccess ? func(result.Value) : Result.Failure(result.Error);
    }

    #endregion

    #region Async Transformations (TransformAsync / BindAsync / ThenAsync)

    /// <summary>
    /// Transforms the value of a successful result using an asynchronous function.
    /// </summary>
    public static async Task<Result<TOut>> TransformAsync<TIn, TOut>(this Result<TIn> result, Func<TIn, Task<TOut>> func)
    {
        return result.IsSuccess ? Result.Success(await func(result.Value)) : Result.Failure<TOut>(result.Error);
    }

    /// <summary>
    /// Transforms the value of a successful task result using a synchronous function.
    /// </summary>
    public static async Task<Result<TOut>> TransformAsync<TIn, TOut>(this Task<Result<TIn>> resultTask, Func<TIn, TOut> func)
    {
        var result = await resultTask;
        return result.Transform(func);
    }

    /// <summary>
    /// Transforms the value of a successful task result using an asynchronous function.
    /// </summary>
    public static async Task<Result<TOut>> TransformAsync<TIn, TOut>(this Task<Result<TIn>> resultTask, Func<TIn, Task<TOut>> func)
    {
        var result = await resultTask;
        return result.IsSuccess ? Result.Success(await func(result.Value)) : Result.Failure<TOut>(result.Error);
    }



    /// <summary>
    /// Chains an asynchronous operation that returns a <see cref="Result"/> if the current result is successful.
    /// </summary>
    public static async Task<Result> ThenAsync(this Result result, Func<Task<Result>> func)
    {
        return result.IsSuccess ? await func() : result;
    }

    /// <summary>
    /// Chains an asynchronous operation that returns a <see cref="Result{TOut}"/> if the current result is successful.
    /// </summary>
    public static async Task<Result<TOut>> ThenAsync<TOut>(this Result result, Func<Task<Result<TOut>>> func)
    {
        return result.IsSuccess ? await func() : Result.Failure<TOut>(result.Error);
    }

    /// <summary>
    /// Chains an asynchronous operation that returns a <see cref="Result"/> if the current result is successful.
    /// </summary>
    public static async Task<Result> ThenAsync<TIn>(this Result<TIn> result, Func<TIn, Task<Result>> func)
    {
        return result.IsSuccess ? await func(result.Value) : Result.Failure(result.Error);
    }

    /// <summary>
    /// Chains an asynchronous operation that returns a <see cref="Result{TOut}"/> if the current result is successful.
    /// </summary>
    public static async Task<Result<TOut>> ThenAsync<TIn, TOut>(this Result<TIn> result, Func<TIn, Task<Result<TOut>>> func)
    {
        return result.IsSuccess ? await func(result.Value) : Result.Failure<TOut>(result.Error);
    }

    /// <summary>
    /// Chains a synchronous operation that returns a <see cref="Result"/> if the current task result is successful.
    /// </summary>
    public static async Task<Result> ThenAsync(this Task<Result> resultTask, Func<Result> func)
    {
        var result = await resultTask;
        return result.IsSuccess ? func() : result;
    }

    /// <summary>
    /// Chains a synchronous operation that returns a <see cref="Result{TOut}"/> if the current task result is successful.
    /// </summary>
    public static async Task<Result<TOut>> ThenAsync<TOut>(this Task<Result> resultTask, Func<Result<TOut>> func)
    {
        var result = await resultTask;
        return result.IsSuccess ? func() : Result.Failure<TOut>(result.Error);
    }

    /// <summary>
    /// Chains an asynchronous operation that returns a <see cref="Result"/> if the current task result is successful.
    /// </summary>
    public static async Task<Result> ThenAsync(this Task<Result> resultTask, Func<Task<Result>> func)
    {
        var result = await resultTask;
        return result.IsSuccess ? await func() : result;
    }

    /// <summary>
    /// Chains an asynchronous operation that returns a <see cref="Result{TOut}"/> if the current task result is successful.
    /// </summary>
    public static async Task<Result<TOut>> ThenAsync<TOut>(this Task<Result> resultTask, Func<Task<Result<TOut>>> func)
    {
        var result = await resultTask;
        return result.IsSuccess ? await func() : Result.Failure<TOut>(result.Error);
    }

    /// <summary>
    /// Chains a synchronous operation that returns a <see cref="Result"/> if the current task result is successful.
    /// </summary>
    public static async Task<Result> ThenAsync<TIn>(this Task<Result<TIn>> resultTask, Func<TIn, Result> func)
    {
        var result = await resultTask;
        return result.IsSuccess ? func(result.Value) : Result.Failure(result.Error);
    }

    /// <summary>
    /// Chains a synchronous operation that returns a <see cref="Result{TOut}"/> if the current task result is successful.
    /// </summary>
    public static async Task<Result<TOut>> ThenAsync<TIn, TOut>(this Task<Result<TIn>> resultTask, Func<TIn, Result<TOut>> func)
    {
        var result = await resultTask;
        return result.IsSuccess ? func(result.Value) : Result.Failure<TOut>(result.Error);
    }

    /// <summary>
    /// Chains an asynchronous operation that returns a <see cref="Result"/> if the current task result is successful.
    /// </summary>
    public static async Task<Result> ThenAsync<TIn>(this Task<Result<TIn>> resultTask, Func<TIn, Task<Result>> func)
    {
        var result = await resultTask;
        return result.IsSuccess ? await func(result.Value) : Result.Failure(result.Error);
    }

    /// <summary>
    /// Chains an asynchronous operation that returns a <see cref="Result{TOut}"/> if the current task result is successful.
    /// </summary>
    public static async Task<Result<TOut>> ThenAsync<TIn, TOut>(this Task<Result<TIn>> resultTask, Func<TIn, Task<Result<TOut>>> func)
    {
        var result = await resultTask;
        return result.IsSuccess ? await func(result.Value) : Result.Failure<TOut>(result.Error);
    }

    #endregion

    #region Side Effects (Then)

    /// <summary>
    /// Executes a synchronous action if the result is successful, and returns the original result.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result Then(this Result result, Action action)
    {
        if (result.IsSuccess) action();
        return result;
    }

    /// <summary>
    /// Executes a synchronous action with the value if successful, and returns the original result.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result<TValue> Then<TValue>(this Result<TValue> result, Action<TValue> action)
    {
        if (result.IsSuccess) action(result.Value);
        return result;
    }

    /// <summary>
    /// Executes an asynchronous action if the result is successful, and returns the original result.
    /// </summary>
    public static async Task<Result<TValue>> ThenAsync<TValue>(this Result<TValue> result, Func<TValue, Task> func)
    {
        if (result.IsSuccess) await func(result.Value);
        return result;
    }

    /// <summary>
    /// Executes an asynchronous action if the task result is successful, and returns the original result.
    /// </summary>
    public static async Task<Result<TValue>> ThenAsync<TValue>(this Task<Result<TValue>> resultTask, Func<TValue, Task> func)
    {
        var result = await resultTask;
        if (result.IsSuccess) await func(result.Value);
        return result;
    }

    #endregion

    #region Guards & Conditions (Ensure / When)

    /// <summary>
    /// Ensures that the result value satisfies a condition, otherwise returns a failure.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result<TValue> Ensure<TValue>(this Result<TValue> result, Func<TValue, bool> predicate, Error error)
    {
        if (result.IsFailure) return result;
        return predicate(result.Value) ? result : Result.Failure<TValue>(error);
    }

    /// <summary>
    /// Ensures that the value is not null, otherwise returns a failure.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result<TValue> EnsureNotNull<TValue>(this Result<TValue?> result, Error error) where TValue : class
    {
        if (result.IsFailure) return Result.Failure<TValue>(result.Error);
        return result.ValueOrDefault is not null ? Result.Success(result.ValueOrDefault) : Result.Failure<TValue>(error);
    }

    /// <summary>
    /// Conditionally executes a transformation if a condition is met.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result<TValue> When<TValue>(this Result<TValue> result, Func<TValue, bool> condition, Func<TValue, TValue> func)
    {
        if (result.IsFailure || !condition(result.Value)) return result;
        return Result.Success(func(result.Value));
    }

    /// <summary>
    /// Conditionally executes a result-returning operation if a condition is met.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result<TValue> When<TValue>(this Result<TValue> result, Func<TValue, bool> condition, Func<TValue, Result<TValue>> func)
    {
        if (result.IsFailure || !condition(result.Value)) return result;
        return func(result.Value);
    }

    #endregion

    #region Finalization (Match / OnFailure)

    /// <summary>
    /// Executes an action if the result failed.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result OnFailure(this Result result, Action<Error> action)
    {
        if (result.IsFailure) action(result.Error);
        return result;
    }

    /// <summary>
    /// Executes an action if the task result failed.
    /// </summary>
    public static async Task<Result> OnFailure(this Task<Result> resultTask, Action<Error> action)
    {
        var result = await resultTask;
        if (result.IsFailure) action(result.Error);
        return result;
    }

    /// <summary>
    /// Executes an action if the result failed.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result<TValue> OnFailure<TValue>(this Result<TValue> result, Action<Error> action)
    {
        if (result.IsFailure) action(result.Error);
        return result;
    }

    /// <summary>
    /// Executes an action if the task result failed.
    /// </summary>
    public static async Task<Result<TValue>> OnFailure<TValue>(this Task<Result<TValue>> resultTask, Action<Error> action)
    {
        var result = await resultTask;
        if (result.IsFailure) action(result.Error);
        return result;
    }

    /// <summary>
    /// Executes an asynchronous action if the result failed.
    /// </summary>
    public static async Task<Result> OnFailureAsync(this Result result, Func<Error, Task> func)
    {
        if (result.IsFailure) await func(result.Error);
        return result;
    }

    /// <summary>
    /// Executes an asynchronous action if the result failed.
    /// </summary>
    public static async Task<Result<TValue>> OnFailureAsync<TValue>(this Result<TValue> result, Func<Error, Task> func)
    {
        if (result.IsFailure) await func(result.Error);
        return result;
    }

    /// <summary>
    /// Executes an asynchronous action if the task result failed.
    /// </summary>
    public static async Task<Result> OnFailureAsync(this Task<Result> resultTask, Func<Error, Task> func)
    {
        var result = await resultTask;
        if (result.IsFailure) await func(result.Error);
        return result;
    }

    /// <summary>
    /// Executes an asynchronous action if the task result failed.
    /// </summary>
    public static async Task<Result<TValue>> OnFailureAsync<TValue>(this Task<Result<TValue>> resultTask, Func<Error, Task> func)
    {
        var result = await resultTask;
        if (result.IsFailure) await func(result.Error);
        return result;
    }

    /// <summary>
    /// Matches the result and returns a value based on success or failure.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T Match<T>(this Result result, Func<T> onSuccess, Func<Error, T> onFailure)
    {
        return result.IsSuccess ? onSuccess() : onFailure(result.Error);
    }

    /// <summary>
    /// Matches the generic result and returns a value based on success or failure.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TOut Match<TIn, TOut>(this Result<TIn> result, Func<TIn, TOut> onSuccess, Func<Error, TOut> onFailure)
    {
        return result.IsSuccess ? onSuccess(result.Value) : onFailure(result.Error);
    }

    /// <summary>
    /// Matches the generic result task and returns a value.
    /// </summary>
    public static async Task<TOut> MatchAsync<TIn, TOut>(this Task<Result<TIn>> resultTask, Func<TIn, TOut> onSuccess, Func<Error, TOut> onFailure)
    {
        var result = await resultTask;
        return result.Match(onSuccess, onFailure);
    }

    #endregion
}
