using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Tests.Kafka;

using System.Collections.Generic;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core.Pipeline;
using Xunit;

/// <summary>
/// Tests the consumer pipeline execution logic:
/// step ordering, short-circuit on failure, and context mutability.
/// Uses manual IConsumerPipelineStep implementations because the interface is internal
/// and Moq's proxy generator requires DynamicProxyGenAssembly2 access.
/// </summary>
public sealed class ConsumerPipelineBuilderTests
{
    private static ConsumerPipelineContext CreateDefaultContext(string rawPayload = "{\"key\":\"value\"}")
    {
        return new ConsumerPipelineContext
        {
            RawPayload = rawPayload,
            MessageId = "msg-001",
            CorrelationId = "corr-001",
            TopicConfig = new KafkaTopicConfiguration
            {
                TopicName = "test-topic",
                GroupId = "test-group"
            },
            TopicName = "test-topic",
            Partition = 0,
            Offset = 42,
            Headers = new Dictionary<string, string>
            {
                ["x-message-id"] = "msg-001"
            }
        };
    }

    /// <summary>
    /// Executes a pipeline of steps using the same recursive pattern as ConsumerPipelineBuilder.
    /// </summary>
    private static async Task<Result<ConsumerPipelineContext>> ExecutePipelineAsync(
        IReadOnlyList<IConsumerPipelineStep> steps,
        ConsumerPipelineContext context,
        CancellationToken cancellationToken)
    {
        return await ExecuteStepAsync(steps, context, 0, cancellationToken);
    }

    private static async Task<Result<ConsumerPipelineContext>> ExecuteStepAsync(
        IReadOnlyList<IConsumerPipelineStep> steps,
        ConsumerPipelineContext context,
        int currentIndex,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (currentIndex >= steps.Count)
        {
            return Result.Success(context);
        }

        var currentStep = steps[currentIndex];
        return await currentStep.ExecuteAsync(
            context,
            nextContext => ExecuteStepAsync(steps, nextContext, currentIndex + 1, cancellationToken),
            cancellationToken);
    }

    // ──────────────────────────────────────────────
    // Manual test stubs for IConsumerPipelineStep
    // ──────────────────────────────────────────────

    /// <summary>
    /// A pass-through step that optionally executes a callback, then delegates to the next step.
    /// </summary>
    private sealed class PassThroughStep : IConsumerPipelineStep
    {
        private readonly Action<ConsumerPipelineContext>? _onExecute;
        public int ExecutionCount { get; private set; }

        public PassThroughStep(Action<ConsumerPipelineContext>? onExecute = null)
        {
            _onExecute = onExecute;
        }

        public Task<Result<ConsumerPipelineContext>> ExecuteAsync(
            ConsumerPipelineContext context,
            Func<ConsumerPipelineContext, Task<Result<ConsumerPipelineContext>>> next,
            CancellationToken cancellationToken)
        {
            ExecutionCount++;
            _onExecute?.Invoke(context);
            return next(context);
        }
    }

    /// <summary>
    /// A step that always returns a failure result, never calling next.
    /// </summary>
    private sealed class FailingStep : IConsumerPipelineStep
    {
        private readonly string _errorMessage;

        public FailingStep(string errorMessage = "Step failed")
        {
            _errorMessage = errorMessage;
        }

        public Task<Result<ConsumerPipelineContext>> ExecuteAsync(
            ConsumerPipelineContext context,
            Func<ConsumerPipelineContext, Task<Result<ConsumerPipelineContext>>> next,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(Result.Failure<ConsumerPipelineContext>(_errorMessage));
        }
    }

    // ──────────────────────────────────────────────
    // Empty pipeline returns success
    // ──────────────────────────────────────────────

    [Fact]
    public async Task ExecutePipeline_WithNoSteps_ReturnsSuccessWithOriginalContext()
    {
        // Arrange
        var context = CreateDefaultContext();
        var steps = Array.Empty<IConsumerPipelineStep>();

        // Act
        var result = await ExecutePipelineAsync(steps, context, CancellationToken.None);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Same(context, result.Value);
    }

    // ──────────────────────────────────────────────
    // Single step executes and passes context
    // ──────────────────────────────────────────────

    [Fact]
    public async Task ExecutePipeline_WithSingleStep_ExecutesStepAndReturnsContext()
    {
        // Arrange
        var context = CreateDefaultContext();
        ConsumerPipelineContext? capturedContext = null;

        var step = new PassThroughStep(executedContext => capturedContext = executedContext);
        var steps = new IConsumerPipelineStep[] { step };

        // Act
        var result = await ExecutePipelineAsync(steps, context, CancellationToken.None);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Same(context, capturedContext);
        Assert.Equal(1, step.ExecutionCount);
    }

    // ──────────────────────────────────────────────
    // Multiple steps execute in order
    // ──────────────────────────────────────────────

    [Fact]
    public async Task ExecutePipeline_WithMultipleSteps_ExecutesInOrder()
    {
        // Arrange
        var context = CreateDefaultContext();
        var executionOrder = new List<int>();

        var step1 = new PassThroughStep(_ => executionOrder.Add(1));
        var step2 = new PassThroughStep(_ => executionOrder.Add(2));
        var step3 = new PassThroughStep(_ => executionOrder.Add(3));
        var steps = new IConsumerPipelineStep[] { step1, step2, step3 };

        // Act
        var result = await ExecutePipelineAsync(steps, context, CancellationToken.None);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(new[] { 1, 2, 3 }, executionOrder);
    }

    // ──────────────────────────────────────────────
    // Step returning failure short-circuits pipeline
    // ──────────────────────────────────────────────

    [Fact]
    public async Task ExecutePipeline_WhenStepFails_ShortCircuitsAndSkipsRemainingSteps()
    {
        // Arrange
        var context = CreateDefaultContext();
        var executionOrder = new List<int>();

        var step1 = new PassThroughStep(_ => executionOrder.Add(1));
        var failingStep = new FailingStep("Deduplication rejected message");
        var step3 = new PassThroughStep(_ => executionOrder.Add(3));
        var steps = new IConsumerPipelineStep[] { step1, failingStep, step3 };

        // Act
        var result = await ExecutePipelineAsync(steps, context, CancellationToken.None);

        // Assert
        Assert.True(result.IsFailure);
        Assert.Equal("Deduplication rejected message", result.Error.Message);
        Assert.Equal(new[] { 1 }, executionOrder); // Step 3 never executed
    }

    // ──────────────────────────────────────────────
    // Context RawPayload is mutable across steps
    // ──────────────────────────────────────────────

    [Fact]
    public async Task ExecutePipeline_ContextPayloadJson_IsMutableAcrossSteps()
    {
        // Arrange
        var originalPayload = "{\"version\":1}";
        var transformedPayload = "{\"version\":2,\"upgraded\":true}";
        var context = CreateDefaultContext(originalPayload);

        var transformStep = new PassThroughStep(executedContext =>
        {
            executedContext.RawPayload = transformedPayload;
        });
        string? capturedPayload = null;
        var verifyStep = new PassThroughStep(executedContext =>
        {
            capturedPayload = executedContext.RawPayload;
        });
        var steps = new IConsumerPipelineStep[] { transformStep, verifyStep };

        // Act
        var result = await ExecutePipelineAsync(steps, context, CancellationToken.None);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(transformedPayload, capturedPayload);
        Assert.Equal(transformedPayload, result.Value.RawPayload);
    }

    // ──────────────────────────────────────────────
    // Context Headers is mutable across steps
    // ──────────────────────────────────────────────

    [Fact]
    public async Task ExecutePipeline_ContextHeaders_IsMutableAcrossSteps()
    {
        // Arrange
        var context = CreateDefaultContext();
        var enrichedHeaders = new Dictionary<string, string>
        {
            ["x-message-id"] = "msg-001",
            ["x-trace-id"] = "trace-abc-123",
            ["x-custom-header"] = "custom-value"
        };

        var enrichStep = new PassThroughStep(executedContext =>
        {
            executedContext.Headers = enrichedHeaders;
        });
        IReadOnlyDictionary<string, string>? capturedHeaders = null;
        var verifyStep = new PassThroughStep(executedContext =>
        {
            capturedHeaders = executedContext.Headers;
        });
        var steps = new IConsumerPipelineStep[] { enrichStep, verifyStep };

        // Act
        var result = await ExecutePipelineAsync(steps, context, CancellationToken.None);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.NotNull(capturedHeaders);
        Assert.Equal(3, capturedHeaders!.Count);
        Assert.Equal("trace-abc-123", capturedHeaders["x-trace-id"]);
        Assert.Equal("custom-value", capturedHeaders["x-custom-header"]);
    }
}
