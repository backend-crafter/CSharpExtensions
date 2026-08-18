using CSharpExtensions.Foundation.Helpers.Constants;
using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Core.Pipeline;

using System;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using RailwayError = Error;

/// <summary>
/// Pipeline step that intercepts multi-segment messages and reassembles them using the registered message assembler.
/// If the message is not fully assembled, the pipeline is short-circuited.
/// </summary>
internal sealed class MessageAssemblyStep : IConsumerPipelineStep
{
    private readonly IMessageAssembler _messageAssembler;

    /// <summary>
    /// Initializes a new instance of the <see cref="MessageAssemblyStep"/> class.
    /// </summary>
    /// <param name="messageAssembler">The message assembler instance.</param>
    public MessageAssemblyStep(IMessageAssembler messageAssembler)
    {
        _messageAssembler = messageAssembler ?? throw new ArgumentNullException(nameof(messageAssembler));
    }

    /// <inheritdoc />
    public async Task<Result<ConsumerPipelineContext>> ExecuteAsync(
        ConsumerPipelineContext context,
        Func<ConsumerPipelineContext, Task<Result<ConsumerPipelineContext>>> next,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Check if assembly headers exist in context
        var hasAssemblyKey = context.Headers.TryGetValue(CustomRequestHeaders.AssemblyKey, out var assemblyKey);
        var hasSegmentIndex = context.Headers.TryGetValue(CustomRequestHeaders.SegmentIndex, out var segmentIndexHeader);
        var hasTotalSegments = context.Headers.TryGetValue(CustomRequestHeaders.TotalSegments, out var totalSegmentsHeader);

        // If no assembly headers are present, pass through to the next step
        if (!hasAssemblyKey || !hasSegmentIndex || !hasTotalSegments)
        {
            return await next(context);
        }

        if (string.IsNullOrWhiteSpace(assemblyKey))
        {
            return Result.Failure<ConsumerPipelineContext>(
                new RailwayError("Assembly key header must not be empty.")
                    .AsBadRequest("Validation", "Missing assembly key."));
        }

        if (!int.TryParse(segmentIndexHeader, out var segmentIndex) ||
            !int.TryParse(totalSegmentsHeader, out var totalSegments))
        {
            return Result.Failure<ConsumerPipelineContext>(
                new RailwayError("Invalid assembly headers.")
                    .AsBadRequest("Validation", "Assembly headers must be valid integers."));
        }

        var assemblyResult = await _messageAssembler.TryAssembleAsync(
            context.RawPayload,
            assemblyKey,
            segmentIndex,
            totalSegments,
            cancellationToken);

        if (!assemblyResult.IsSuccess)
        {
            return Result.Failure<ConsumerPipelineContext>(assemblyResult.Error);
        }

        var assembledPayload = assemblyResult.Value;

        // If the payload is not yet fully assembled, short-circuit processing
        if (assembledPayload is null)
        {
            return Result.Failure<ConsumerPipelineContext>(
                new RailwayError("Segment stored, awaiting remaining parts.")
                    .AsInternalServer("AwaitingSegments", "Segment stored, awaiting remaining parts."));
        }

        // Update the context payload and continue
        context.RawPayload = assembledPayload;
        return await next(context);
    }
}
