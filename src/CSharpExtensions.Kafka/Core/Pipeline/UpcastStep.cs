using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Core.Pipeline;

using System;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Evolution;

/// <summary>
/// Applies schema evolution upcasting to transform messages from older schema versions.
/// </summary>
internal sealed class UpcastStep : IConsumerPipelineStep
{
    private readonly MessageUpcastRegistry _upcastRegistry;
    private readonly string _targetSchemaName;

    /// <summary>
    /// Initializes a new instance of the <see cref="UpcastStep"/> class.
    /// </summary>
    /// <param name="upcastRegistry">The registry containing registered upcasters.</param>
    /// <param name="targetSchemaName">The target schema name for upcast resolution.</param>
    public UpcastStep(MessageUpcastRegistry upcastRegistry, string targetSchemaName)
    {
        _upcastRegistry = upcastRegistry ?? throw new ArgumentNullException(nameof(upcastRegistry));
        _targetSchemaName = targetSchemaName ?? throw new ArgumentNullException(nameof(targetSchemaName));
    }

    /// <inheritdoc />
    public Task<Result<ConsumerPipelineContext>> ExecuteAsync(
        ConsumerPipelineContext context,
        Func<ConsumerPipelineContext, Task<Result<ConsumerPipelineContext>>> next,
        CancellationToken cancellationToken)
    {
        if (!MessageVersionResolver.TryResolveSourceSchemaKey(
                context.SchemaVersionKey,
                context.RawPayload,
                out var sourceKey))
        {
            return Task.FromResult(Result.Failure<ConsumerPipelineContext>(
                "Kafka source schema version is invalid."));
        }

        var upcastResult = _upcastRegistry.UpcastMessage(
            context.RawPayload, sourceKey, _targetSchemaName);

        if (!upcastResult.IsSuccess)
        {
            return Task.FromResult(Result.Failure<ConsumerPipelineContext>(upcastResult.Error));
        }

        context.RawPayload = upcastResult.Value;
        return next(context);
    }
}
