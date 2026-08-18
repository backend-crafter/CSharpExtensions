using CSharpExtensions.Core.Railway;
using CSharpExtensions.Core.Security.Interfaces;

namespace CSharpExtensions.Tests.Kafka;

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using CSharpExtensions.Kafka.Evolution;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

public sealed class KafkaMessageBusTests
{
    private readonly Mock<IServiceProvider> _mockServiceProvider;
    private readonly Mock<IDistributedDuplicateDetector> _mockDuplicateDetector;
    private readonly Mock<ILogger<KafkaMessageBus>> _mockLogger;
    private readonly Mock<ILogger<KafkaProducerManager>> _mockProducerLogger;
    private readonly Mock<IEncryptionService> _mockEncryptionService;
    private readonly SignatureService _signatureService;
    private readonly S3ClaimCheckOffloader _offloader;
    private readonly MessageUpcastRegistry _upcastRegistry;

    public KafkaMessageBusTests()
    {
        _mockServiceProvider = new Mock<IServiceProvider>();
        _mockDuplicateDetector = new Mock<IDistributedDuplicateDetector>();
        _mockLogger = new Mock<ILogger<KafkaMessageBus>>();
        _mockProducerLogger = new Mock<ILogger<KafkaProducerManager>>();
        _mockEncryptionService = new Mock<IEncryptionService>();
        
        _signatureService = new SignatureService(_mockEncryptionService.Object);
        _offloader = new S3ClaimCheckOffloader(null, null);
        _upcastRegistry = new MessageUpcastRegistry(new List<IMessageUpcaster>());
    }

    [Fact]
    public void ConsumerTaskKey_IsModeIndependentAndCollisionSafe()
    {
        var shared = new KafkaMessageBus.ConsumerTaskKey("events.test.v1", "service.group");
        var sameTopicAndGroup = new KafkaMessageBus.ConsumerTaskKey("events.test.v1", "service.group");
        var formerlyAmbiguousLeft = new KafkaMessageBus.ConsumerTaskKey("events:test", "group");
        var formerlyAmbiguousRight = new KafkaMessageBus.ConsumerTaskKey("events", "test:group");

        Assert.Equal(shared, sameTopicAndGroup);
        Assert.NotEqual(formerlyAmbiguousLeft, formerlyAmbiguousRight);
    }

    [Fact]
    public async Task CancelPendingConsumerStart_ReleasesStartGateAndCancelsConsumer()
    {
        using var cancellation = new CancellationTokenSource();
        var startGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        KafkaMessageBus.CancelPendingConsumerStart(cancellation, startGate);

        Assert.True(cancellation.IsCancellationRequested);
        Assert.True(await startGate.Task.WaitAsync(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void FatalConsumeFailure_IsEscalatedToConsumerSupervision()
    {
        var fatal = new ConsumeException(
            new ConsumeResult<byte[], byte[]>(),
            new Confluent.Kafka.Error(ErrorCode.Local_Transport, "fatal", isFatal: true));
        var recoverable = new ConsumeException(
            new ConsumeResult<byte[], byte[]>(),
            new Confluent.Kafka.Error(ErrorCode.Local_Transport, "recoverable", isFatal: false));

        Assert.True(KafkaMessageBus.IsFatalConsumeFailure(fatal));
        Assert.False(KafkaMessageBus.IsFatalConsumeFailure(recoverable));
    }

    [Fact]
    public async Task PublishAsync_WithUnconfiguredConfigurationKey_ReturnsFailure()
    {
        // Arrange
        var options = new KafkaOptions();
        var producerManager = new KafkaProducerManager(Options.Create(options), _mockProducerLogger.Object);
        var bus = new KafkaMessageBus(
            _mockServiceProvider.Object,
            producerManager,
            _mockDuplicateDetector.Object,
            _signatureService,
            _offloader,
            _upcastRegistry,
            Options.Create(options),
            _mockLogger.Object);

        var testMessage = new TestMessage { Data = "test" };

        // Act
        var result = await bus.PublishAsync(testMessage);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Contains("is not defined", result.Error.Message);
    }

    [Fact]
    public void ResolveSubscription_WithUnconfiguredConfigurationKey_ReturnsFailure()
    {
        // Arrange
        var options = new KafkaOptions();
        using var producerManager = new KafkaProducerManager(Options.Create(options), _mockProducerLogger.Object);
        var bus = new KafkaMessageBus(
            _mockServiceProvider.Object,
            producerManager,
            _mockDuplicateDetector.Object,
            _signatureService,
            _offloader,
            _upcastRegistry,
            Options.Create(options),
            _mockLogger.Object);

        var subscriptionOptions = new KafkaSubscriptionOptions();

        // Act — use reflection to invoke the internal method
        var method = typeof(KafkaMessageBus)
            .GetMethod("ResolveSubscription", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(TestMessage));

        var result = (Result<(KafkaTopicConfiguration, KafkaClusterConfiguration, string)>)method.Invoke(
            bus,
            [subscriptionOptions])!;

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Contains("is not defined", result.Error.Message);
    }

    [Fact]
    public async Task SubscribeConsumerAsync_WithMismatchedClusterConfigAndNoFallback_ReturnsFailure()
    {
        // Arrange
        var options = new KafkaOptions
        {
            Topics = new Dictionary<string, KafkaTopicConfiguration>
            {
                ["TestMessage"] = new KafkaTopicConfiguration
                {
                    TopicName = "test-topic",
                    Cluster = "NonExistentCluster",
                    Permission = TopicPermission.Read
                }
            }
        };
        var producerManager = new KafkaProducerManager(Options.Create(options), _mockProducerLogger.Object);
        var bus = new KafkaMessageBus(
            _mockServiceProvider.Object,
            producerManager,
            _mockDuplicateDetector.Object,
            _signatureService,
            _offloader,
            _upcastRegistry,
            Options.Create(options),
            _mockLogger.Object);

        var subscriptionOptions = new KafkaSubscriptionOptions();

        // Act — use reflection to invoke the internal method
        var method = typeof(KafkaMessageBus)
            .GetMethod("SubscribeConsumerAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(TestMessage));

        var resultTask = (Task<Result>)method.Invoke(bus, new object[] { subscriptionOptions, CancellationToken.None })!;
        var result = await resultTask;

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Contains("no root 'Servers' fallback is configured", result.Error.Message);
    }

    [Fact]
    public void ResolveSubscription_WithFallbackServersConfig_ResolvesSuccessfully()
    {
        // Arrange
        var options = new KafkaOptions
        {
            Servers = "localhost:9092",
            Topics = new Dictionary<string, KafkaTopicConfiguration>
            {
                ["TestMessage"] = new KafkaTopicConfiguration
                {
                    TopicName = "test-topic",
                    Cluster = "Default",
                    Permission = TopicPermission.Read,
                    GroupId = "test-group"
                }
            }
        };
        using var producerManager = new KafkaProducerManager(Options.Create(options), _mockProducerLogger.Object);
        var bus = new KafkaMessageBus(
            _mockServiceProvider.Object,
            producerManager,
            _mockDuplicateDetector.Object,
            _signatureService,
            _offloader,
            _upcastRegistry,
            Options.Create(options),
            _mockLogger.Object);

        var subscriptionOptions = new KafkaSubscriptionOptions();

        // Act — use reflection to invoke the internal method
        var method = typeof(KafkaMessageBus)
            .GetMethod("ResolveSubscription", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(TestMessage));

        var result = (Result<(KafkaTopicConfiguration, KafkaClusterConfiguration, string)>)method.Invoke(
            bus,
            [subscriptionOptions])!;

        // Assert
        Assert.True(result.IsSuccess);
    }

    [Fact]
    public async Task ProcessHandlerMessageAsync_WhenCompleteClaimFails_ThrowsExceptionAndDoesNotCommit()
    {
        // Arrange
        var options = new KafkaOptions();
        var producerManager = new KafkaProducerManager(Options.Create(options), _mockProducerLogger.Object);
        var bus = new KafkaMessageBus(
            _mockServiceProvider.Object,
            producerManager,
            _mockDuplicateDetector.Object,
            _signatureService,
            _offloader,
            _upcastRegistry,
            Options.Create(options),
            _mockLogger.Object);

        var consumeResult = new ConsumeResult<string, string>
        {
            Topic = "test-topic",
            Partition = new Partition(0),
            Offset = new Offset(100),
            Message = new Message<string, string> { Key = "key", Value = "{ \"Data\": \"test\" }" }
        };

        var headers = new ConsumedMessageHeaders
        {
            MessageId = "msg-123",
            HasValidMessageIdHeader = true,
            CorrelationId = "corr-123",
            SchemaVersionKey = "TestMessage",
            RawHeaders = new Dictionary<string, string>()
        };

        var topicConfig = new KafkaTopicConfiguration
        {
            TopicName = "test-topic",
            IsIdempotent = true,
            IdempotencyRetentionSeconds = 60
        };

        // CompleteClaimAsync returns failure (lease lost)
        _mockDuplicateDetector.Setup(d => d.CompleteClaimAsync(
            "msg-123",
            "group-1",
            It.IsAny<CancellationToken>(),
            60,
            It.IsAny<string>()))
            .ReturnsAsync(Result.Failure("Lease lost."));

        _mockDuplicateDetector.Setup(d => d.TryClaimUniqueAsync(
            "msg-123",
            "group-1",
            60,
            It.IsAny<CancellationToken>(),
            It.IsAny<string>()))
            .ReturnsAsync(Result.Success(true));

        var mockConsumer = new Mock<IConsumer<string, string>>();
        var mockHandler = new Mock<IMessageHandler<TestMessage>>();
        mockHandler.Setup(h => h.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Success());

        var mockScope = new Mock<IServiceScope>();
        var mockScopeFactory = new Mock<IServiceScopeFactory>();
        var mockScopeServiceProvider = new Mock<IServiceProvider>();

        mockScope.Setup(s => s.ServiceProvider).Returns(mockScopeServiceProvider.Object);
        mockScopeFactory.Setup(f => f.CreateScope()).Returns(mockScope.Object);

        _mockServiceProvider.Setup(s => s.GetService(typeof(IServiceScopeFactory)))
            .Returns(mockScopeFactory.Object);

        mockScopeServiceProvider.Setup(s => s.GetService(typeof(IMessageHandler<TestMessage>)))
            .Returns(mockHandler.Object);

        var method = typeof(KafkaMessageBus)
            .GetMethod("ProcessHandlerMessageAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(TestMessage), typeof(IMessageHandler<TestMessage>));

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var task = (Task<bool>)method.Invoke(bus, new object[] { consumeResult, headers, topicConfig, "group-1", mockConsumer.Object, CancellationToken.None })!;
            await task;
        });

        Assert.Contains("Critical failure completing idempotency claim in Redis", exception.Message);
        mockConsumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, string>>()), Times.Never);
    }

    [Fact]
    public async Task ProcessHandlerMessageAsync_WhenHandlerFails_ReleasesClaimAndDoesNotCommit()
    {
        // Arrange
        var options = new KafkaOptions();
        var producerManager = new KafkaProducerManager(Options.Create(options), _mockProducerLogger.Object);
        var bus = new KafkaMessageBus(
            _mockServiceProvider.Object,
            producerManager,
            _mockDuplicateDetector.Object,
            _signatureService,
            _offloader,
            _upcastRegistry,
            Options.Create(options),
            _mockLogger.Object);

        var consumeResult = new ConsumeResult<string, string>
        {
            Topic = "test-topic",
            Partition = new Partition(0),
            Offset = new Offset(100),
            Message = new Message<string, string> { Key = "key", Value = "{ \"Data\": \"test\" }" }
        };

        var headers = new ConsumedMessageHeaders
        {
            MessageId = "msg-123",
            HasValidMessageIdHeader = true,
            CorrelationId = "corr-123",
            SchemaVersionKey = "TestMessage",
            RawHeaders = new Dictionary<string, string>()
        };

        var topicConfig = new KafkaTopicConfiguration
        {
            TopicName = "test-topic",
            IsIdempotent = true,
            IdempotencyRetentionSeconds = 60,
            EnableDlq = false
        };

        _mockDuplicateDetector.Setup(d => d.TryClaimUniqueAsync(
            "msg-123",
            "group-1",
            60,
            It.IsAny<CancellationToken>(),
            It.IsAny<string>()))
            .ReturnsAsync(Result.Success(true));

        var mockConsumer = new Mock<IConsumer<string, string>>();
        var mockHandler = new Mock<IMessageHandler<TestMessage>>();
        mockHandler.Setup(h => h.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Failure("Business validation failed."));

        var mockScope = new Mock<IServiceScope>();
        var mockScopeFactory = new Mock<IServiceScopeFactory>();
        var mockScopeServiceProvider = new Mock<IServiceProvider>();

        mockScope.Setup(s => s.ServiceProvider).Returns(mockScopeServiceProvider.Object);
        mockScopeFactory.Setup(f => f.CreateScope()).Returns(mockScope.Object);

        _mockServiceProvider.Setup(s => s.GetService(typeof(IServiceScopeFactory)))
            .Returns(mockScopeFactory.Object);

        mockScopeServiceProvider.Setup(s => s.GetService(typeof(IMessageHandler<TestMessage>)))
            .Returns(mockHandler.Object);

        var method = typeof(KafkaMessageBus)
            .GetMethod("ProcessHandlerMessageAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(TestMessage), typeof(IMessageHandler<TestMessage>));

        // Act & Assert (DLQ is disabled, so it should throw)
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var task = (Task<bool>)method.Invoke(bus, new object[] { consumeResult, headers, topicConfig, "group-1", mockConsumer.Object, CancellationToken.None })!;
            await task;
        });

        // Verify ReleaseClaimAsync was called, and Commit was NEVER called
        _mockDuplicateDetector.Verify(d => d.ReleaseClaimAsync(
            "msg-123",
            "group-1",
            It.IsAny<CancellationToken>(),
            It.IsAny<string>()), Times.Once);

        mockConsumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, string>>()), Times.Never);
    }

    [Fact]
    public void Extract_WhenMessageIdHeaderIsMissing_DoesNotGenerateFallbackWhenDisabled()
    {
        var consumeResult = CreateConsumeResult();

        var headers = ConsumedMessageHeaders.Extract<TestMessage>(
            consumeResult,
            allowGeneratedMessageIdFallback: false);

        Assert.False(headers.HasValidMessageIdHeader);
        Assert.Empty(headers.MessageId);
    }

    [Fact]
    public void Extract_WhenMessageIdHeaderIsMissing_PreservesLegacyFallbackWhenEnabled()
    {
        var consumeResult = CreateConsumeResult();

        var headers = ConsumedMessageHeaders.Extract<TestMessage>(consumeResult);

        Assert.False(headers.HasValidMessageIdHeader);
        Assert.True(Guid.TryParse(headers.MessageId, out _));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("message\tid")]
    public void Extract_WhenMessageIdHeaderIsInvalid_RejectsRecord(string messageId)
    {
        var consumeResult = CreateConsumeResult(messageId: Encoding.UTF8.GetBytes(messageId));

        Assert.Throws<System.IO.InvalidDataException>(() =>
            ConsumedMessageHeaders.Extract<TestMessage>(
                consumeResult,
                allowGeneratedMessageIdFallback: false));
    }

    [Fact]
    public void Extract_WhenMessageIdHeaderHasInvalidUtf8_RejectsIt()
    {
        var consumeResult = CreateConsumeResult(messageId: new byte[] { 0xC3, 0x28 });

        Assert.Throws<System.IO.InvalidDataException>(() =>
            ConsumedMessageHeaders.Extract<TestMessage>(
                consumeResult,
                allowGeneratedMessageIdFallback: false));
    }

    [Fact]
    public void Extract_WhenMessageIdHeaderIsTooLarge_RejectsIt()
    {
        var consumeResult = CreateConsumeResult(messageId: Encoding.UTF8.GetBytes(new string('a', 257)));

        Assert.Throws<System.IO.InvalidDataException>(() =>
            ConsumedMessageHeaders.Extract<TestMessage>(
                consumeResult,
                allowGeneratedMessageIdFallback: false));
    }

    [Fact]
    public void Extract_WhenMessageIdHeaderIsValid_PreservesOpaqueIdentifier()
    {
        const string expectedMessageId = "message-2026_08:tenant.42";
        var consumeResult = CreateConsumeResult(messageId: Encoding.UTF8.GetBytes(expectedMessageId));

        var headers = ConsumedMessageHeaders.Extract<TestMessage>(
            consumeResult,
            allowGeneratedMessageIdFallback: false);

        Assert.True(headers.HasValidMessageIdHeader);
        Assert.Equal(expectedMessageId, headers.MessageId);
    }

    [Fact]
    public void Extract_WhenProtectedHeaderIsDuplicated_RejectsRecord()
    {
        var consumeResult = CreateConsumeResult(messageId: Encoding.UTF8.GetBytes("first"));
        consumeResult.Message.Headers.Add("X-Message-Id", Encoding.UTF8.GetBytes("second"));

        Assert.Throws<System.IO.InvalidDataException>(() =>
            ConsumedMessageHeaders.Extract<TestMessage>(
                consumeResult,
                collectRawHeaders: true,
                allowGeneratedMessageIdFallback: false));
    }

    [Fact]
    public void Extract_WhenHeaderKeyContainsCrLf_RejectsWithoutReflectingKey()
    {
        var consumeResult = CreateConsumeResult();
        consumeResult.Message.Headers.Add("bad\r\nkey", Encoding.UTF8.GetBytes("value"));

        var exception = Assert.Throws<System.IO.InvalidDataException>(() =>
            ConsumedMessageHeaders.Extract<TestMessage>(consumeResult, collectRawHeaders: true));

        Assert.Equal("Kafka message contains an invalid header key.", exception.Message);
        Assert.DoesNotContain("bad", exception.Message);
    }

    [Fact]
    public async Task ProcessHandlerMessageAsync_WhenIdempotentMessageIdIsMissing_HaltsBeforeClaimOrCommit()
    {
        var bus = CreateBus();
        var consumeResult = CreateConsumeResult();
        var headers = ConsumedMessageHeaders.Extract<TestMessage>(
            consumeResult,
            allowGeneratedMessageIdFallback: false);
        var topicConfig = new KafkaTopicConfiguration
        {
            TopicName = "test-topic",
            IsIdempotent = true
        };
        var consumer = new Mock<IConsumer<string, string>>();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var task = (Task<bool>)typeof(KafkaMessageBus)
                .GetMethod("ProcessHandlerMessageAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
                .MakeGenericMethod(typeof(TestMessage), typeof(TestMessageHandler))
                .Invoke(bus, new object[] { consumeResult, headers, topicConfig, "group-1", consumer.Object, CancellationToken.None })!;
            await task;
        });

        Assert.Contains("requires exactly one valid x-message-id", exception.Message);
        _mockDuplicateDetector.VerifyNoOtherCalls();
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, string>>()), Times.Never);
    }

    [Fact]
    public async Task ProcessChannelMessageAsync_WhenIdempotentMessageIdIsInvalid_HaltsBeforeClaimOrCommit()
    {
        var bus = CreateBus();
        var consumeResult = CreateConsumeResult(messageId: Encoding.UTF8.GetBytes(" "));
        var headers = new ConsumedMessageHeaders
        {
            MessageId = string.Empty,
            HasValidMessageIdHeader = false,
            CorrelationId = Guid.NewGuid().ToString(),
            SchemaVersionKey = nameof(TestMessage),
            RawHeaders = new Dictionary<string, string>()
        };
        var topicConfig = new KafkaTopicConfiguration
        {
            TopicName = "test-topic",
            IsIdempotent = true
        };
        var consumer = new Mock<IConsumer<string, string>>();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var task = (Task<bool>)typeof(KafkaMessageBus)
                .GetMethod("ProcessChannelMessageAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
                .MakeGenericMethod(typeof(TestMessage))
                .Invoke(bus, new object[] { consumeResult, headers, topicConfig, "group-1", consumer.Object, CancellationToken.None })!;
            await task;
        });

        Assert.Contains("requires exactly one valid x-message-id", exception.Message);
        _mockDuplicateDetector.VerifyNoOtherCalls();
        consumer.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, string>>()), Times.Never);
    }

    private KafkaMessageBus CreateBus()
    {
        var options = new KafkaOptions();
        using var producerManager = new KafkaProducerManager(Options.Create(options), _mockProducerLogger.Object);
        return new KafkaMessageBus(
            _mockServiceProvider.Object,
            producerManager,
            _mockDuplicateDetector.Object,
            _signatureService,
            _offloader,
            _upcastRegistry,
            Options.Create(options),
            _mockLogger.Object);
    }

    private static ConsumeResult<string, string> CreateConsumeResult(byte[]? messageId = null)
    {
        var headers = new Headers();
        if (messageId is not null)
        {
            headers.Add("x-message-id", messageId);
        }

        return new ConsumeResult<string, string>
        {
            Topic = "test-topic",
            Partition = new Partition(0),
            Offset = new Offset(100),
            Message = new Message<string, string>
            {
                Key = "key",
                Value = "{ \"Data\": \"test\" }",
                Headers = headers
            }
        };
    }
}

public class TestMessage
{
    public string Data { get; set; } = null!;
}

public class TestMessageHandler : IMessageHandler<TestMessage>
{
    public Task<Result> HandleAsync(TestMessage message, CancellationToken cancellationToken)
    {
        return Task.FromResult(Result.Success());
    }
}
