using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Core.Pipeline;

using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.Options;

using RailwayError = Error;

/// <summary>
/// Downloads offloaded payloads from S3 when the message is a claim check reference.
/// Uses structured JSON parsing to detect claim check markers.
/// </summary>
internal sealed class S3DownloadStep : IConsumerPipelineStep
{
    private readonly S3ClaimCheckOffloader _offloader;
    private readonly KafkaOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="S3DownloadStep"/> class.
    /// </summary>
    /// <param name="offloader">The S3 claim check offloader for downloading payloads.</param>
    /// <param name="options">The Kafka options containing offloading configuration.</param>
    public S3DownloadStep(S3ClaimCheckOffloader offloader, IOptions<KafkaOptions> options)
    {
        _offloader = offloader ?? throw new ArgumentNullException(nameof(offloader));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public async Task<Result<ConsumerPipelineContext>> ExecuteAsync(
        ConsumerPipelineContext context,
        Func<ConsumerPipelineContext, Task<Result<ConsumerPipelineContext>>> next,
        CancellationToken cancellationToken)
    {
        if (context.TopicConfig.ResolvedStrategy != LargePayloadStrategy.S3Offloading)
        {
            return await next(context);
        }

        // Quick pre-filter: skip JSON parsing for messages that are not claim check payloads
        if (!context.RawPayload.Contains("\"$ref\"", StringComparison.Ordinal))
        {
            return await next(context);
        }

        if (Encoding.UTF8.GetByteCount(context.RawPayload) > 16 * 1024)
        {
            return Result.Failure<ConsumerPipelineContext>("Kafka claim-check envelope exceeds the permitted size.");
        }

        try
        {
            using var jsonDocument = JsonDocument.Parse(
                context.RawPayload,
                new JsonDocumentOptions { MaxDepth = 16 });

            if (!jsonDocument.RootElement.TryGetProperty("$ref", out var refElement) ||
                refElement.ValueKind != JsonValueKind.True)
            {
                return await next(context);
            }

            var downloadResult = await _offloader.DownloadAsync(
                jsonDocument.RootElement, _options.Offloading, cancellationToken);

            if (!downloadResult.IsSuccess)
            {
                return Result.Failure<ConsumerPipelineContext>(downloadResult.Error);
            }

            context.RawPayload = downloadResult.Value;
            return await next(context);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (JsonException)
        {
            return Result.Failure<ConsumerPipelineContext>(
                new RailwayError("Kafka claim-check envelope is invalid JSON."));
        }
    }
}
