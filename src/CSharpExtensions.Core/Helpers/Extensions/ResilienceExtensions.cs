namespace CSharpExtensions.Core.Helpers.Extensions;

/// <summary>
/// Provides bounded exponential-backoff resiliency extensions.
/// </summary>
public static class ResilienceExtensions
{
    private static readonly TimeSpan MaximumSupportedDelay = TimeSpan.FromMilliseconds(int.MaxValue - 1d);

    /// <summary>
    /// Executes the specified asynchronous action with a retry policy using exponential backoff.
    /// </summary>
    public static async Task<T> TryAgainAsync<T>(
        this Func<CancellationToken, Task<T>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Exception, bool>? shouldRetry = null,
        double backoffMultiplier = 2.0,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteAsync(
            action,
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
            maxDelay: null,
            useJitter: false,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes an asynchronous action with bounded exponential backoff and optional jitter.
    /// </summary>
    public static Task<T> TryAgainWithPolicyAsync<T>(
        this Func<CancellationToken, Task<T>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        TimeSpan maxDelay,
        Func<Exception, bool> shouldRetry,
        double backoffMultiplier = 2.0,
        bool useJitter = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(shouldRetry);
        return ExecuteAsync(
            action,
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
            maxDelay,
            useJitter,
            cancellationToken);
    }

    private static async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Exception, bool>? shouldRetry,
        double backoffMultiplier,
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
                return await action(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (attempt < maxAttempts && exception is not OperationCanceledException)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (shouldRetry is null || !shouldRetry(exception))
                {
                    throw;
                }

                await Task.Delay(ApplyJitter(currentDelay, useJitter), cancellationToken).ConfigureAwait(false);
                currentDelay = CalculateNextDelay(currentDelay, backoffMultiplier, maxDelay);
            }
        }
    }

    /// <summary>
    /// Executes the specified asynchronous action with a retry policy using exponential backoff.
    /// </summary>
    public static Task<T> TryAgainAsync<T>(
        this Func<Task<T>> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Exception, bool>? shouldRetry = null,
        double backoffMultiplier = 2.0,
        CancellationToken cancellationToken = default)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return TryAgainAsync(
            _ => action(),
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
            cancellationToken);
    }

    /// <summary>
    /// Executes the specified asynchronous action with a retry policy using exponential backoff.
    /// </summary>
    public static async Task TryAgainAsync(
        this Func<CancellationToken, Task> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Exception, bool>? shouldRetry = null,
        double backoffMultiplier = 2.0,
        CancellationToken cancellationToken = default)
    {
        await ExecuteAsync(
            action,
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
            maxDelay: null,
            useJitter: false,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes an asynchronous action with bounded exponential backoff and optional jitter.
    /// </summary>
    public static Task TryAgainWithPolicyAsync(
        this Func<CancellationToken, Task> action,
        int maxAttempts,
        TimeSpan initialDelay,
        TimeSpan maxDelay,
        Func<Exception, bool> shouldRetry,
        double backoffMultiplier = 2.0,
        bool useJitter = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(shouldRetry);
        return ExecuteAsync(
            action,
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
            maxDelay,
            useJitter,
            cancellationToken);
    }

    private static async Task ExecuteAsync(
        Func<CancellationToken, Task> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Exception, bool>? shouldRetry,
        double backoffMultiplier,
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
                await action(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception exception) when (attempt < maxAttempts && exception is not OperationCanceledException)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (shouldRetry is null || !shouldRetry(exception))
                {
                    throw;
                }

                await Task.Delay(ApplyJitter(currentDelay, useJitter), cancellationToken).ConfigureAwait(false);
                currentDelay = CalculateNextDelay(currentDelay, backoffMultiplier, maxDelay);
            }
        }
    }

    /// <summary>
    /// Executes the specified asynchronous action with a retry policy using exponential backoff.
    /// </summary>
    public static Task TryAgainAsync(
        this Func<Task> action,
        int maxAttempts,
        TimeSpan initialDelay,
        Func<Exception, bool>? shouldRetry = null,
        double backoffMultiplier = 2.0,
        CancellationToken cancellationToken = default)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return TryAgainAsync(
            _ => action(),
            maxAttempts,
            initialDelay,
            shouldRetry,
            backoffMultiplier,
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
