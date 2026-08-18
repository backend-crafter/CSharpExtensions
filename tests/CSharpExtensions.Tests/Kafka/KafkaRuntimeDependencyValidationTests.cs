namespace CSharpExtensions.Tests.Kafka;

using System;
using System.Collections.Generic;
using System.Threading;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

public sealed class KafkaRuntimeDependencyValidationTests
{
    [Fact]
    public async Task StartAsync_RegionalClientMismatch_FailsStartup()
    {
        var options = CreateS3Options("eu-west-1");
        using var provider = CreateServiceProvider(RegionEndpoint.USEast1, serviceUrl: null);
        var validator = new KafkaRuntimeDependencyValidationHostedService(
            Options.Create(options),
            provider);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => validator.StartAsync(CancellationToken.None));

        Assert.Equal(
            "Kafka S3 offloading region does not match the registered IAmazonS3 client region.",
            exception.Message);
    }

    [Fact]
    public async Task StartAsync_CustomServiceUrl_AllowsApplicationOwnedEndpoint()
    {
        var options = CreateS3Options("eu-west-1");
        using var provider = CreateServiceProvider(
            RegionEndpoint.USEast1,
            "https://s3.internal.example");
        var validator = new KafkaRuntimeDependencyValidationHostedService(
            Options.Create(options),
            provider);

        await validator.StartAsync(CancellationToken.None);
    }

    private static KafkaOptions CreateS3Options(string region)
    {
        return new KafkaOptions
        {
            Offloading = new KafkaOffloadOptions
            {
                Region = region,
                BucketName = "test-bucket"
            },
            Topics = new Dictionary<string, KafkaTopicConfiguration>
            {
                ["TestMessage"] = new()
                {
                    TopicName = "events.test.changed.v1",
                    LargePayloadStrategy = LargePayloadStrategy.S3Offloading
                }
            }
        };
    }

    private static ServiceProvider CreateServiceProvider(
        RegionEndpoint regionEndpoint,
        string? serviceUrl)
    {
        var clientConfig = new Mock<IClientConfig>();
        clientConfig.SetupGet(config => config.RegionEndpoint).Returns(regionEndpoint);
        clientConfig.SetupGet(config => config.ServiceURL).Returns(serviceUrl!);
        var s3Client = new Mock<IAmazonS3>();
        s3Client.SetupGet(client => client.Config).Returns(clientConfig.Object);

        return new ServiceCollection()
            .AddSingleton(s3Client.Object)
            .BuildServiceProvider();
    }
}
