namespace CSharpExtensions.Tests.Kafka;

using System;
using Confluent.Kafka;
using CSharpExtensions.Kafka.Extensions;
using Xunit;

public sealed class KafkaErrorExtensionsTests
{
    [Fact]
    public void KafkaError_WithKafkaException_MapsCorrectly()
    {
        // Arrange
        var kafkaError = new Error(ErrorCode.Local_Transport, "Connection failed", isFatal: true);
        var exception = new KafkaException(kafkaError);

        // Act
        var error = KafkaErrorExtensions.KafkaError(exception);

        // Assert
        Assert.NotNull(error);
        Assert.Equal("Kafka client operation failed.", error.Message);
        Assert.Equal("A Kafka client error occurred.", error.Title);
        Assert.Equal("Kafka.ClientError", error.Type);
        Assert.Equal(500, error.HttpStatusCode);
        Assert.Equal(ErrorCode.Local_Transport.ToString(), error.Metadata["ErrorCode"]);
        Assert.False(error.Metadata.ContainsKey("Reason"));
        Assert.Equal(true, error.Metadata["IsFatal"]);
        Assert.Empty(error.Details);
    }

    [Fact]
    public void KafkaConsumeError_WithConsumeException_MapsCorrectly()
    {
        // Arrange
        var kafkaError = new Error(ErrorCode.Local_BadMsg, "Bad message format", isFatal: false);
        var consumeResult = new ConsumeResult<byte[], byte[]>();
        var exception = new ConsumeException(consumeResult, kafkaError);

        // Act
        var error = KafkaErrorExtensions.KafkaConsumeError(exception);

        // Assert
        Assert.NotNull(error);
        Assert.Equal("Kafka consume operation failed.", error.Message);
        Assert.Equal("An error occurred while consuming messages from Kafka.", error.Title);
        Assert.Equal("Kafka.ConsumeError", error.Type);
        Assert.Equal(500, error.HttpStatusCode);
        Assert.Equal(ErrorCode.Local_BadMsg.ToString(), error.Metadata["ErrorCode"]);
        Assert.False(error.Metadata.ContainsKey("Reason"));
        Assert.Equal(false, error.Metadata["IsFatal"]);
        Assert.Equal(true, error.Metadata["IsLocalError"]);
        Assert.Empty(error.Details);
    }

    [Fact]
    public void KafkaConsumeError_WithGeneralException_MapsToUnknownError()
    {
        // Arrange
        var exception = new InvalidOperationException("Something went wrong");

        // Act
        var error = KafkaErrorExtensions.KafkaConsumeError(exception);

        // Assert
        Assert.NotNull(error);
        Assert.Equal("Kafka consumer operation failed.", error.Message);
        Assert.Equal("An unexpected Kafka error occurred.", error.Title);
        Assert.Equal("Kafka.UnknownError", error.Type);
        Assert.Equal(500, error.HttpStatusCode);
        Assert.Empty(error.Details);
    }

    [Fact]
    public void KafkaConsumeError_WithInnerException_DoesNotExposeExceptionMessages()
    {
        // Arrange
        var inner = new Exception("Inner connection issue");
        var exception = new InvalidOperationException("Outer message", inner);

        // Act
        var error = KafkaErrorExtensions.KafkaConsumeError(exception);

        // Assert
        Assert.NotNull(error);
        Assert.Equal("Kafka consumer operation failed.", error.Message);
        Assert.Equal(nameof(InvalidOperationException), error.Metadata["ErrorType"]);
        Assert.Empty(error.Details);
    }

    [Fact]
    public void KafkaConsumeError_WithKafkaExceptionInstance_MapsToClientError()
    {
        // Arrange
        var kafkaError = new Error(ErrorCode.Local_State, "Invalid state", isFatal: true);
        var exception = new KafkaException(kafkaError);

        // Act
        var error = KafkaErrorExtensions.KafkaConsumeError((Exception)exception);

        // Assert
        Assert.NotNull(error);
        Assert.Equal("Kafka client operation failed.", error.Message);
        Assert.Equal("A Kafka client error occurred.", error.Title);
        Assert.Equal("Kafka.ClientError", error.Type);
        Assert.Equal(500, error.HttpStatusCode);
        Assert.Equal(ErrorCode.Local_State.ToString(), error.Metadata["ErrorCode"]);
        Assert.False(error.Metadata.ContainsKey("Reason"));
        Assert.Equal(true, error.Metadata["IsFatal"]);
        Assert.Empty(error.Details);
    }

    [Fact]
    public void KafkaConsumeError_WithConsumeExceptionInstance_MapsToConsumeError()
    {
        // Arrange
        var kafkaError = new Error(ErrorCode.BrokerNotAvailable, "Broker is down", isFatal: true);
        var consumeResult = new ConsumeResult<byte[], byte[]>();
        var exception = new ConsumeException(consumeResult, kafkaError);

        // Act
        var error = KafkaErrorExtensions.KafkaConsumeError((Exception)exception);

        // Assert
        Assert.NotNull(error);
        Assert.Equal("Kafka consume operation failed.", error.Message);
        Assert.Equal("An error occurred while consuming messages from Kafka.", error.Title);
        Assert.Equal("Kafka.ConsumeError", error.Type);
        Assert.Equal(500, error.HttpStatusCode);
        Assert.Equal(ErrorCode.BrokerNotAvailable.ToString(), error.Metadata["ErrorCode"]);
        Assert.False(error.Metadata.ContainsKey("Reason"));
        Assert.Equal(true, error.Metadata["IsFatal"]);
        Assert.Equal(false, error.Metadata["IsLocalError"]);
        Assert.Empty(error.Details);
    }
}
