namespace CSharpExtensions.Core.Railway.Extensions;

/// <summary>
/// Provides resilient retry extensions for Railway Oriented Programming (ROP) result flows.
/// </summary>
public static class ResultResilienceExtensions
{
    private static readonly TimeSpan MaximumSupportedDelay = TimeSpan.FromMilliseconds(int.MaxValue - 1d);

    /// <summary>
    /// Executes a result-returning asynchronous operation with a retry policy based on ROP errors and optional exception predicates.
    /// </summary>
    public static async Task<Result<T>> TryResultAgainAsync<T>(
        this Func<CancellationToken, Task<Result<T>>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Error, bool>? shouldRetry = null,
        double backoffMultiplier = 2.0,
        Func<Exception, bool>? shouldRetryException = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteAsync(
            action,
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
            shouldRetryException,
            maxDelay: null,
            useJitter: false,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a result-returning operation with bounded backoff and explicit retry policies.
    /// </summary>
    public static Task<Result<T>> TryResultAgainWithPolicyAsync<T>(
        this Func<CancellationToken, Task<Result<T>>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        TimeSpan maxDelay,
        Func<Error, bool> shouldRetry,
        Func<Exception, bool> shouldRetryException,
        double backoffMultiplier = 2.0,
        bool useJitter = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(shouldRetry);
        ArgumentNullException.ThrowIfNull(shouldRetryException);
        return ExecuteAsync(
            action,
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
            shouldRetryException,
            maxDelay,
            useJitter,
            cancellationToken);
    }

    private static async Task<Result<T>> ExecuteAsync<T>(
        Func<CancellationToken, Task<Result<T>>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Error, bool>? shouldRetry,
        double backoffMultiplier,
        Func<Exception, bool>? shouldRetryException,
        TimeSpan? maxDelay,
        bool useJitter,
        CancellationToken cancellationToken)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        ValidatePolicy(maxAttempts, initialDelay, backoffMultiplier, maxDelay);

        var currentDelay = initialDelay;

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                var result = await action(cancellationToken).ConfigureAwait(false);

                if (result.IsSuccess || attempt >= maxAttempts)
                {
                    return result;
                }

                if (shouldRetry is null || !shouldRetry(result.Error))
                {
                    return result;
                }

                await Task.Delay(ApplyJitter(currentDelay, useJitter), cancellationToken).ConfigureAwait(false);
                currentDelay = CalculateNextDelay(currentDelay, backoffMultiplier, maxDelay);
            }
            catch (Exception exception) when (attempt < maxAttempts && exception is not OperationCanceledException)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (shouldRetryException == null || !shouldRetryException(exception))
                {
                    throw;
                }

                await Task.Delay(ApplyJitter(currentDelay, useJitter), cancellationToken).ConfigureAwait(false);
                currentDelay = CalculateNextDelay(currentDelay, backoffMultiplier, maxDelay);
            }
        }
    }

    /// <summary>
    /// Executes a result-returning asynchronous operation with a retry policy based on ROP errors.
    /// </summary>
    public static Task<Result<T>> TryResultAgainAsync<T>(
        this Func<Task<Result<T>>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Error, bool>? shouldRetry = null,
        double backoffMultiplier = 2.0,
        Func<Exception, bool>? shouldRetryException = null,
        CancellationToken cancellationToken = default)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return TryResultAgainAsync(
            _ => action(),
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
            shouldRetryException,
            cancellationToken);
    }

    /// <summary>
    /// Executes a result-returning asynchronous operation with a retry policy based on ROP errors and optional exception predicates.
    /// </summary>
    public static async Task<Result> TryResultAgainAsync(
        this Func<CancellationToken, Task<Result>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Error, bool>? shouldRetry = null,
        double backoffMultiplier = 2.0,
        Func<Exception, bool>? shouldRetryException = null,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteAsync(
            action,
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
            shouldRetryException,
            maxDelay: null,
            useJitter: false,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a result-returning operation with bounded backoff and explicit retry policies.
    /// </summary>
    public static Task<Result> TryResultAgainWithPolicyAsync(
        this Func<CancellationToken, Task<Result>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        TimeSpan maxDelay,
        Func<Error, bool> shouldRetry,
        Func<Exception, bool> shouldRetryException,
        double backoffMultiplier = 2.0,
        bool useJitter = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(shouldRetry);
        ArgumentNullException.ThrowIfNull(shouldRetryException);
        return ExecuteAsync(
            action,
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
            shouldRetryException,
            maxDelay,
            useJitter,
            cancellationToken);
    }

    private static async Task<Result> ExecuteAsync(
        Func<CancellationToken, Task<Result>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Error, bool>? shouldRetry,
        double backoffMultiplier,
        Func<Exception, bool>? shouldRetryException,
        TimeSpan? maxDelay,
        bool useJitter,
        CancellationToken cancellationToken)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        ValidatePolicy(maxAttempts, initialDelay, backoffMultiplier, maxDelay);

        var currentDelay = initialDelay;

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                var result = await action(cancellationToken).ConfigureAwait(false);

                if (result.IsSuccess || attempt >= maxAttempts)
                {
                    return result;
                }

                if (shouldRetry is null || !shouldRetry(result.Error))
                {
                    return result;
                }

                await Task.Delay(ApplyJitter(currentDelay, useJitter), cancellationToken).ConfigureAwait(false);
                currentDelay = CalculateNextDelay(currentDelay, backoffMultiplier, maxDelay);
            }
            catch (Exception exception) when (attempt < maxAttempts && exception is not OperationCanceledException)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (shouldRetryException == null || !shouldRetryException(exception))
                {
                    throw;
                }

                await Task.Delay(ApplyJitter(currentDelay, useJitter), cancellationToken).ConfigureAwait(false);
                currentDelay = CalculateNextDelay(currentDelay, backoffMultiplier, maxDelay);
            }
        }
    }

    /// <summary>
    /// Executes a result-returning asynchronous operation with a retry policy based on ROP errors.
    /// </summary>
    public static Task<Result> TryResultAgainAsync(
        this Func<Task<Result>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Error, bool>? shouldRetry = null,
        double backoffMultiplier = 2.0,
        Func<Exception, bool>? shouldRetryException = null,
        CancellationToken cancellationToken = default)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return TryResultAgainAsync(
            _ => action(),
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
            shouldRetryException,
            cancellationToken);
    }

    private static void ValidatePolicy(
        int maxAttempts,
        TimeSpan initialDelay,
        double backoffMultiplier,
        TimeSpan? maxDelay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        if (initialDelay < TimeSpan.Zero || initialDelay > MaximumSupportedDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(initialDelay));
        }

        if (!double.IsFinite(backoffMultiplier) || backoffMultiplier < 1d)
        {
            throw new ArgumentOutOfRangeException(nameof(backoffMultiplier));
        }

        if (maxDelay is { } configuredMaxDelay &&
            (configuredMaxDelay < initialDelay || configuredMaxDelay > MaximumSupportedDelay))
        {
            throw new ArgumentOutOfRangeException(nameof(maxDelay));
        }
    }

    private static TimeSpan CalculateNextDelay(
        TimeSpan currentDelay,
        double backoffMultiplier,
        TimeSpan? maxDelay)
    {
        var limit = maxDelay ?? MaximumSupportedDelay;
        var milliseconds = currentDelay.TotalMilliseconds * backoffMultiplier;
        return !double.IsFinite(milliseconds) || milliseconds >= limit.TotalMilliseconds
            ? limit
            : TimeSpan.FromMilliseconds(milliseconds);
    }

    private static TimeSpan ApplyJitter(TimeSpan delay, bool useJitter)
    {
        if (!useJitter || delay == TimeSpan.Zero)
        {
            return delay;
        }

        var factor = 0.5d + (Random.Shared.NextDouble() * 0.5d);
        return TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * factor, MaximumSupportedDelay.TotalMilliseconds));
    }
}
