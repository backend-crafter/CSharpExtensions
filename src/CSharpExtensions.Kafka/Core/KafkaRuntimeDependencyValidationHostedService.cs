namespace CSharpExtensions.Kafka.Core;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

/// <summary>
/// Validates optional runtime dependencies before any Kafka background publisher or consumer starts.
/// </summary>
internal sealed class KafkaRuntimeDependencyValidationHostedService(
    IOptions<KafkaOptions> options,
    IServiceProvider serviceProvider) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var requiresS3 = options.Value.Topics.Values.Any(
            topic => topic.ResolvedStrategy == LargePayloadStrategy.S3Offloading);
        if (!requiresS3)
        {
            return Task.CompletedTask;
        }

        var s3Client = serviceProvider.GetService<IAmazonS3>();
        if (s3Client is null)
        {
            throw new InvalidOperationException(
                "Kafka S3 offloading is enabled, but IAmazonS3 is not registered in dependency injection.");
        }

        var clientConfig = s3Client.Config;
        if (string.IsNullOrWhiteSpace(clientConfig.ServiceURL))
        {
            var clientRegion = clientConfig.RegionEndpoint?.SystemName;
            if (string.IsNullOrWhiteSpace(clientRegion)
                || !string.Equals(
                    clientRegion,
                    options.Value.Offloading.Region,
                    StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    "Kafka S3 offloading region does not match the registered IAmazonS3 client region.");
            }
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
