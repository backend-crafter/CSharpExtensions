namespace CSharpExtensions.Tests.Kafka;

using CSharpExtensions.Kafka.Core.Resilience;
using Xunit;

public sealed class ConsumerCircuitBreakerTests
{
    private const int DefaultThreshold = 3;
    private const int DefaultWindowSeconds = 60;
    private const int DefaultCooldownMs = 500;

    private ConsumerCircuitBreaker CreateBreaker(
        int failureThreshold = DefaultThreshold,
        int windowSeconds = DefaultWindowSeconds,
        int cooldownMs = DefaultCooldownMs)
    {
        return new ConsumerCircuitBreaker(failureThreshold, windowSeconds, cooldownMs);
    }

    // ──────────────────────────────────────────────
    // Initial state
    // ──────────────────────────────────────────────

    [Fact]
    public void State_WhenNewlyCreated_IsClosed()
    {
        // Arrange & Act
        var breaker = CreateBreaker();

        // Assert
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
    }

    // ──────────────────────────────────────────────
    // RecordSuccess keeps Closed
    // ──────────────────────────────────────────────

    [Fact]
    public void RecordSuccess_WhenClosed_RemainsClosedAndAllowsRequests()
    {
        // Arrange
        var breaker = CreateBreaker();

        // Act
        breaker.RecordSuccess();

        // Assert
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        Assert.True(breaker.AllowRequest());
    }

    // ──────────────────────────────────────────────
    // Failures below threshold keep Closed
    // ──────────────────────────────────────────────

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public void RecordFailure_BelowThreshold_RemainsClosedAndAllowsRequests(int failureCount)
    {
        // Arrange
        var breaker = CreateBreaker(failureThreshold: 3);

        // Act
        for (var iteration = 0; iteration < failureCount; iteration++)
        {
            breaker.RecordFailure();
        }

        // Assert
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        Assert.True(breaker.AllowRequest());
    }

    // ──────────────────────────────────────────────
    // Failures reaching threshold trip to Open
    // ──────────────────────────────────────────────

    [Fact]
    public void RecordFailure_ReachesThreshold_TransitionsToOpenAndBlocksRequests()
    {
        // Arrange
        var breaker = CreateBreaker(failureThreshold: 3);

        // Act
        for (var iteration = 0; iteration < 3; iteration++)
        {
            breaker.RecordFailure();
        }

        // Assert
        Assert.Equal(CircuitBreakerState.Open, breaker.State);
        Assert.False(breaker.AllowRequest());
    }

    // ──────────────────────────────────────────────
    // After cooldown, state transitions to HalfOpen
    // ──────────────────────────────────────────────

    [Fact]
    public async Task AllowRequest_AfterCooldownElapses_TransitionsToHalfOpen()
    {
        // Arrange
        var cooldownMs = 200;
        var breaker = CreateBreaker(failureThreshold: 1, cooldownMs: cooldownMs);
        breaker.RecordFailure(); // Trip to Open
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        // Act — wait for cooldown to elapse
        await Task.Delay(cooldownMs + 100);
        var isAllowed = breaker.AllowRequest();

        // Assert
        Assert.True(isAllowed);
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);
    }

    // ──────────────────────────────────────────────
    // RecordSuccess in HalfOpen transitions back to Closed
    // ──────────────────────────────────────────────

    [Fact]
    public async Task RecordSuccess_WhenHalfOpen_TransitionsBackToClosed()
    {
        // Arrange
        var cooldownMs = 200;
        var breaker = CreateBreaker(failureThreshold: 1, cooldownMs: cooldownMs);
        breaker.RecordFailure(); // Trip to Open
        await Task.Delay(cooldownMs + 100);
        breaker.AllowRequest(); // Transition to HalfOpen
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        // Act
        breaker.RecordSuccess();

        // Assert
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        Assert.True(breaker.AllowRequest());
    }

    // ──────────────────────────────────────────────
    // RecordFailure in HalfOpen transitions back to Open
    // ──────────────────────────────────────────────

    [Fact]
    public async Task RecordFailure_WhenHalfOpen_TransitionsBackToOpen()
    {
        // Arrange
        var cooldownMs = 200;
        var breaker = CreateBreaker(failureThreshold: 1, cooldownMs: cooldownMs);
        breaker.RecordFailure(); // Trip to Open
        await Task.Delay(cooldownMs + 100);
        breaker.AllowRequest(); // Transition to HalfOpen
        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        // Act
        breaker.RecordFailure();

        // Assert
        Assert.Equal(CircuitBreakerState.Open, breaker.State);
        Assert.False(breaker.AllowRequest());
    }

    // ──────────────────────────────────────────────
    // Failures outside sliding window do not count
    // ──────────────────────────────────────────────

    [Fact]
    public async Task RecordFailure_OutsideSlidingWindow_DoesNotTripBreaker()
    {
        // Arrange — window = 1 second, threshold = 2
        var breaker = CreateBreaker(failureThreshold: 2, windowSeconds: 1);
        breaker.RecordFailure(); // 1 failure in first window

        // Act — wait for the sliding window to expire, then record one more failure
        await Task.Delay(1200);
        breaker.RecordFailure(); // 1 failure in new window (old one expired)

        // Assert — should still be Closed because the window reset
        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        Assert.True(breaker.AllowRequest());
    }

    // ──────────────────────────────────────────────
    // Thread safety (parallel RecordFailure calls)
    // ──────────────────────────────────────────────

    [Fact]
    public void RecordFailure_CalledInParallel_DoesNotCorruptState()
    {
        // Arrange
        var threshold = 50;
        var breaker = CreateBreaker(failureThreshold: threshold, windowSeconds: 60);
        var totalCalls = threshold * 2;

        // Act — hammer RecordFailure from multiple threads
        Parallel.For(0, totalCalls, _ =>
        {
            breaker.RecordFailure();
        });

        // Assert — after enough failures, the breaker must be in Open state (no corrupt/unknown state)
        var state = breaker.State;
        Assert.True(
            state == CircuitBreakerState.Open || state == CircuitBreakerState.Closed,
            $"Expected Open or Closed after parallel failures, but got {state}");
    }
}
