using CSharpExtensions.Core.Json;
using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Core.Pipeline;

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RailwayError = Error;

/// <summary>
/// Pipeline step that deserializes the raw string payload into the target message type <typeparamref name="TMessage"/>.
/// The resulting deserialized object is stored in the context's Message property, and processing is passed down the pipeline.
/// </summary>
/// <typeparam name="TMessage">The target message type.</typeparam>
internal sealed class DeserializationStep<TMessage> : IConsumerPipelineStep
    where TMessage : class
{
    /// <inheritdoc />
    public async Task<Result<ConsumerPipelineContext>> ExecuteAsync(
        ConsumerPipelineContext context,
        Func<ConsumerPipelineContext, Task<Result<ConsumerPipelineContext>>> next,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            var messageInstance = JsonSerializer.Deserialize<TMessage>(context.RawPayload, JsonOptions.KafkaCompatible);
            if (messageInstance is null)
            {
                return Result.Failure<ConsumerPipelineContext>(
                    new RailwayError("Failed deserializing payload JSON to target message schema model.")
                        .AsBadRequest("ValidationError", "Deserialization returned null."));
            }

            // Verify message version matches topic version suffix
            var topicName = context.TopicConfig.TopicName;
            var topicSegments = topicName.Split('.');
            var topicVersionSegment = topicSegments.Last();
            if (topicVersionSegment.StartsWith("v") && int.TryParse(topicVersionSegment.Substring(1), out var topicVersion))
            {
                var messageVersion = MessageVersionResolver.GetMessageVersion(messageInstance);
                if (messageVersion != topicVersion)
                {
                    return Result.Failure<ConsumerPipelineContext>(
                        new RailwayError("Kafka message version does not match the configured topic version.")
                            .AsBadRequest("Kafka.VersionMismatch", "VersionMismatch"));
                }
            }

            context.Message = messageInstance;

            // Pass the context to the next step
            return await next(context);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            return Result.Failure<ConsumerPipelineContext>(
                new RailwayError("Kafka payload deserialization failed.")
                    .AsBadRequest("Kafka.DeserializationFailed", "Deserialization failed.")
                    .CausedBy(exception));
        }
        catch (Exception exception)
        {
            return Result.Failure<ConsumerPipelineContext>(
                new RailwayError("Kafka payload deserialization failed unexpectedly.")
                    .AsInternalServer("Kafka.DeserializationSystemError", "An unexpected error occurred during deserialization.")
                    .CausedBy(exception));
        }
    }
}
