using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Core.Pipeline;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Defines a single step in the consumer message processing pipeline.
/// Each step can transform the context, short-circuit processing, or pass control to the next step.
/// </summary>
internal interface IConsumerPipelineStep
{
    /// <summary>
    /// Executes this pipeline step.
    /// </summary>
    /// <param name="context">The mutable pipeline context carrying payload and metadata.</param>
    /// <param name="next">Delegate to invoke the next step in the pipeline.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Result wrapping the (possibly transformed) context.</returns>
    Task<Result<ConsumerPipelineContext>> ExecuteAsync(
        ConsumerPipelineContext context,
        Func<ConsumerPipelineContext, Task<Result<ConsumerPipelineContext>>> next,
        CancellationToken cancellationToken);
}
