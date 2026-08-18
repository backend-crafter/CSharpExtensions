using CSharpExtensions.Foundation.Railway;
using CSharpExtensions.Kafka.Abstractions;

namespace CSharpExtensions.Kafka.Core.Pipeline;

using System;
using System.Threading;
using System.Threading.Tasks;
using RailwayError = Error;

/// <summary>
/// Redis-backed deduplication step. Claims unique message processing rights
/// when idempotency is enabled on the topic.
/// </summary>
internal sealed class DeduplicationStep : IConsumerPipelineStep
{
    private readonly IDistributedDuplicateDetector _duplicateDetector;

    /// <summary>
    /// Initializes a new instance of the <see cref="DeduplicationStep"/> class.
    /// </summary>
    /// <param name="duplicateDetector">The distributed duplicate detector for idempotency checks.</param>
    public DeduplicationStep(IDistributedDuplicateDetector duplicateDetector)
    {
        _duplicateDetector = duplicateDetector ?? throw new ArgumentNullException(nameof(duplicateDetector));
    }

    /// <inheritdoc />
    public async Task<Result<ConsumerPipelineContext>> ExecuteAsync(
        ConsumerPipelineContext context,
        Func<ConsumerPipelineContext, Task<Result<ConsumerPipelineContext>>> next,
        CancellationToken cancellationToken)
    {
        if (!context.TopicConfig.IsIdempotent)
        {
            return await next(context);
        }

        var claimResult = await _duplicateDetector.TryClaimUniqueAsync(
            context.MessageId,
            context.ConsumerGroup,
            context.TopicConfig.IdempotencyRetentionSeconds,
            cancellationToken);

        if (!claimResult.IsSuccess)
        {
            return Result.Failure<ConsumerPipelineContext>(claimResult.Error);
        }

        if (!claimResult.Value)
        {
            return Result.Failure<ConsumerPipelineContext>(
                new RailwayError("A duplicate Kafka message was detected.")
                    .AsInternalServer("Conflict", "Duplicate message detected."));
        }

        return await next(context);
    }
}
