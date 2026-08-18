using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Core.Pipeline;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Evolution;
using Microsoft.Extensions.Options;

/// <summary>
/// Assembles the consumer processing pipeline from individual steps.
/// Steps are composed in order: Signature -> S3Download -> Dedup -> Upcast.
/// Each step is conditionally active based on topic configuration.
/// </summary>
public sealed class ConsumerPipelineBuilder
{
    private readonly SignatureService _signatureService;
    private readonly S3ClaimCheckOffloader _offloader;
    private readonly IDistributedDuplicateDetector _duplicateDetector;
    private readonly MessageUpcastRegistry _upcastRegistry;
    private readonly IOptions<KafkaOptions> _options;
    private readonly IMessageAssembler? _messageAssembler;

    /// <summary>
    /// Initializes a new instance of the <see cref="ConsumerPipelineBuilder"/> class.
    /// </summary>
    /// <param name="signatureService">The signature service for message authentication.</param>
    /// <param name="offloader">The S3 claim check offloader for large payload downloads.</param>
    /// <param name="duplicateDetector">The distributed duplicate detector for idempotency.</param>
    /// <param name="upcastRegistry">The message upcast registry for schema evolution.</param>
    /// <param name="options">The Kafka options containing offloading and other configuration.</param>
    /// <param name="messageAssembler">The message assembler instance (optional).</param>
    public ConsumerPipelineBuilder(
        SignatureService signatureService,
        S3ClaimCheckOffloader offloader,
        IDistributedDuplicateDetector duplicateDetector,
        MessageUpcastRegistry upcastRegistry,
        IOptions<KafkaOptions> options,
        IMessageAssembler? messageAssembler = null)
    {
        _signatureService = signatureService ?? throw new ArgumentNullException(nameof(signatureService));
        _offloader = offloader ?? throw new ArgumentNullException(nameof(offloader));
        _duplicateDetector = duplicateDetector ?? throw new ArgumentNullException(nameof(duplicateDetector));
        _upcastRegistry = upcastRegistry ?? throw new ArgumentNullException(nameof(upcastRegistry));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _messageAssembler = messageAssembler;
    }

    /// <summary>
    /// Builds the consumer pipeline for a given message type.
    /// The pipeline processes: Signature -> S3Download -> Dedup -> Upcast -> MessageAssembly -> Deserialization.
    /// Each step checks its enabled flag internally and short-circuits if disabled.
    /// </summary>
    /// <typeparam name="TMessage">The target message type for schema resolution.</typeparam>
    /// <returns>A compiled delegate that processes a ConsumerPipelineContext through all steps.</returns>
    public Func<ConsumerPipelineContext, CancellationToken, Task<Result<ConsumerPipelineContext>>> Build<TMessage>()
        where TMessage : class
    {
        var targetVersion = MessageVersionResolver.GetMessageVersion<TMessage>();
        var steps = new List<IConsumerPipelineStep>
        {
            new SignatureVerificationStep(_signatureService),
            new S3DownloadStep(_offloader, _options),
            new DeduplicationStep(_duplicateDetector),
            new UpcastStep(_upcastRegistry, typeof(TMessage).Name)
        };

        if (_messageAssembler is not null)
        {
            steps.Add(new MessageAssemblyStep(_messageAssembler));
        }

        steps.Add(new DeserializationStep<TMessage>());

        return (context, cancellationToken) => ExecutePipelineAsync(steps, context, 0, cancellationToken);
    }

    private static async Task<Result<ConsumerPipelineContext>> ExecutePipelineAsync(
        IReadOnlyList<IConsumerPipelineStep> steps,
        ConsumerPipelineContext context,
        int currentIndex,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (currentIndex >= steps.Count)
        {
            // All steps completed successfully
            return Result.Success(context);
        }

        var currentStep = steps[currentIndex];
        var stepName = currentStep.GetType().Name;

        using var activity = KafkaDiagnostics.ActivitySource.StartActivity($"CSharpExtensions.Kafka.pipeline.{stepName.ToLowerInvariant()}");
        activity?.SetTag("messaging.kafka.step", stepName);

        try
        {
            var result = await currentStep.ExecuteAsync(
                context,
                nextContext => ExecutePipelineAsync(steps, nextContext, currentIndex + 1, cancellationToken),
                cancellationToken);

            if (!result.IsSuccess)
            {
                activity?.SetStatus(ActivityStatusCode.Error, "Kafka pipeline step failed.");
                activity?.SetTag("otel.status_code", "ERROR");
                activity?.SetTag("otel.status_description", "Kafka pipeline step failed.");
                activity?.SetTag("error.type", result.Error.Type);
            }

            return result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error, "Kafka pipeline step threw an exception.");
            activity?.SetTag("otel.status_code", "ERROR");
            activity?.SetTag("otel.status_description", "Kafka pipeline step threw an exception.");
            activity?.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
            {
                { "exception.type", exception.GetType().FullName }
            }));
            throw;
        }
    }
}
