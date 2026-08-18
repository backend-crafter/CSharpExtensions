namespace CSharpExtensions.Kafka.Core.Resilience;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Thread-safe circuit breaker for Kafka consumer loops.
/// Tracks failure rate in a sliding time window and transitions between states:
/// Closed (normal) -> Open (paused) -> HalfOpen (probing) -> Closed.
/// </summary>
internal sealed class ConsumerCircuitBreaker
{
    private readonly int _failureThreshold;
    private readonly TimeSpan _windowDuration;
    private readonly TimeSpan _cooldownDuration;
    private readonly object _lock = new();

    private CircuitBreakerState _state = CircuitBreakerState.Closed;
    private DateTime _openedAtUtc = DateTime.MinValue;
    private int _failureCount;
    private DateTime _windowStartUtc;

    /// <summary>
    /// Initializes a new instance of the <see cref="ConsumerCircuitBreaker"/> class.
    /// </summary>
    /// <param name="failureThreshold">Number of failures within the window to trip the breaker.</param>
    /// <param name="windowSeconds">Duration of the sliding window in seconds.</param>
    /// <param name="cooldownMs">Cooldown period in milliseconds when the breaker is open.</param>
    public ConsumerCircuitBreaker(int failureThreshold, int windowSeconds, int cooldownMs)
    {
        if (failureThreshold <= 0) throw new ArgumentOutOfRangeException(nameof(failureThreshold));
        if (windowSeconds <= 0) throw new ArgumentOutOfRangeException(nameof(windowSeconds));
        if (cooldownMs <= 0) throw new ArgumentOutOfRangeException(nameof(cooldownMs));

        _failureThreshold = failureThreshold;
        _windowDuration = TimeSpan.FromSeconds(windowSeconds);
        _cooldownDuration = TimeSpan.FromMilliseconds(cooldownMs);
        _windowStartUtc = DateTime.UtcNow;
    }

    /// <summary>
    /// Gets the current state of the circuit breaker.
    /// </summary>
    public CircuitBreakerState State
    {
        get
        {
            lock (_lock)
            {
                return _state;
            }
        }
    }

    /// <summary>
    /// Checks whether the circuit breaker allows a request to proceed.
    /// If the breaker is open and cooldown has elapsed, transitions to HalfOpen.
    /// </summary>
    /// <returns>True if the request is allowed, false if the circuit is open.</returns>
    public bool AllowRequest()
    {
        lock (_lock)
        {
            switch (_state)
            {
                case CircuitBreakerState.Closed:
                    return true;

                case CircuitBreakerState.HalfOpen:
                    return true;

                case CircuitBreakerState.Open:
                    if (DateTime.UtcNow - _openedAtUtc >= _cooldownDuration)
                    {
                        _state = CircuitBreakerState.HalfOpen;
                        return true;
                    }
                    return false;

                default:
                    return true;
            }
        }
    }

    /// <summary>
    /// Records a successful operation. Resets the circuit breaker to Closed state.
    /// </summary>
    public void RecordSuccess()
    {
        lock (_lock)
        {
            _state = CircuitBreakerState.Closed;
            _failureCount = 0;
            _windowStartUtc = DateTime.UtcNow;
        }
    }

    /// <summary>
    /// Records a failed operation. If the failure threshold is reached within the window,
    /// transitions to Open state.
    /// </summary>
    public void RecordFailure()
    {
        lock (_lock)
        {
            // If in HalfOpen and a failure occurs, immediately trip back to Open
            if (_state == CircuitBreakerState.HalfOpen)
            {
                _state = CircuitBreakerState.Open;
                _openedAtUtc = DateTime.UtcNow;
                _failureCount = 0;
                _windowStartUtc = DateTime.UtcNow;
                return;
            }

            // Reset window if it has expired
            var now = DateTime.UtcNow;
            if (now - _windowStartUtc > _windowDuration)
            {
                _failureCount = 0;
                _windowStartUtc = now;
            }

            _failureCount++;

            if (_failureCount >= _failureThreshold)
            {
                _state = CircuitBreakerState.Open;
                _openedAtUtc = now;
                _failureCount = 0;
                _windowStartUtc = now;
            }
        }
    }

    /// <summary>
    /// Waits for the cooldown period to elapse when the circuit breaker is open.
    /// Returns immediately if the circuit is not open.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task WaitForCooldownAsync(CancellationToken cancellationToken)
    {
        TimeSpan remainingCooldown;

        lock (_lock)
        {
            if (_state != CircuitBreakerState.Open)
            {
                return;
            }

            var elapsed = DateTime.UtcNow - _openedAtUtc;
            remainingCooldown = _cooldownDuration - elapsed;
        }

        if (remainingCooldown > TimeSpan.Zero)
        {
            await Task.Delay(remainingCooldown, cancellationToken);
        }
    }
}

/// <summary>
/// Represents the state of a circuit breaker.
/// </summary>
internal enum CircuitBreakerState
{
    /// <summary>Normal operation. Requests are allowed.</summary>
    Closed,

    /// <summary>Breaker is tripped. Requests are blocked until cooldown elapses.</summary>
    Open,

    /// <summary>Probing state. One request is allowed to test if the dependency has recovered.</summary>
    HalfOpen
}
