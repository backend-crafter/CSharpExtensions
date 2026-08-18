using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Core.Pipeline;

using System;
using System.Threading;
using System.Threading.Tasks;
using RailwayError = Error;

/// <summary>
/// Verifies cryptographic message signatures when authentication is enabled.
/// Passes through unchanged when authentication is disabled on the topic.
/// </summary>
internal sealed class SignatureVerificationStep : IConsumerPipelineStep
{
    private readonly SignatureService _signatureService;

    /// <summary>
    /// Initializes a new instance of the <see cref="SignatureVerificationStep"/> class.
    /// </summary>
    /// <param name="signatureService">The signature service used for cryptographic verification.</param>
    public SignatureVerificationStep(SignatureService signatureService)
    {
        _signatureService = signatureService ?? throw new ArgumentNullException(nameof(signatureService));
    }

    /// <inheritdoc />
    public async Task<Result<ConsumerPipelineContext>> ExecuteAsync(
        ConsumerPipelineContext context,
        Func<ConsumerPipelineContext, Task<Result<ConsumerPipelineContext>>> next,
        CancellationToken cancellationToken)
    {
        if (!context.TopicConfig.EnableAuthentication)
        {
            return await next(context);
        }

        if (string.IsNullOrWhiteSpace(context.Signature) ||
            !_signatureService.VerifySignature(
                context.RawPayload,
                context.MessageId,
                context.CorrelationId,
                context.TopicName,
                context.MessageKey,
                context.SchemaVersionKey,
                KafkaMessageBus.ResolveEnvelopeKind(context.RawPayload, context.TopicConfig),
                context.Signature))
        {
            return Result.Failure<ConsumerPipelineContext>(
                new RailwayError("Unauthorized Kafka message received because signature verification failed.")
                    .AsInternalServer("SecurityViolation", "Signature verification failed."));
        }

        return await next(context);
    }
}
