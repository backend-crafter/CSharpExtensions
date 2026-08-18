using CSharpExtensions.Kafka.Core;

namespace CSharpExtensions.Tests.Kafka;

using Amazon.S3;
using Amazon.S3.Model;
using CSharpExtensions.Kafka.Abstractions;
using Moq;
using Xunit;

public sealed class S3ClaimCheckOffloaderTests
{
    [Fact]
    public async Task OffloadAsync_OversizedPayloadIsRejectedBeforeUploadBufferCreation()
    {
        var s3 = new Mock<IAmazonS3>();
        var offloader = new S3ClaimCheckOffloader(s3.Object);
        var options = new KafkaOffloadOptions
        {
            BucketName = "test-bucket",
            MaxDownloadBytes = 8
        };

        var result = await offloader.OffloadAsync(
            new string('x', 1024 * 1024),
            "EventsTestV1",
            options,
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Contains("size", result.Error.Message, StringComparison.OrdinalIgnoreCase);
        s3.Verify(
            client => client.PutObjectAsync(
                It.IsAny<PutObjectRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Theory]
    [InlineData("bad schema")]
    [InlineData("")]
    public async Task OffloadAsync_InvalidSchemaNameIsRejectedBeforeUpload(string schemaName)
    {
        var s3 = new Mock<IAmazonS3>();
        var offloader = new S3ClaimCheckOffloader(s3.Object);
        var options = new KafkaOffloadOptions
        {
            BucketName = "test-bucket",
            MaxDownloadBytes = 1024
        };

        var result = await offloader.OffloadAsync(
            "{}",
            schemaName,
            options,
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Contains("schema", result.Error.Message, StringComparison.OrdinalIgnoreCase);
        s3.Verify(
            client => client.PutObjectAsync(
                It.IsAny<PutObjectRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }
}
