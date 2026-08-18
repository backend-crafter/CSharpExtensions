#pragma warning disable CS0618
namespace CSharpExtensions.Tests.Kafka;

using System.Collections.Generic;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Configuration;
using Xunit;

public class KafkaOptionsValidatorTests
{
    private readonly Moq.Mock<IRedisConnectionResolver> _mockRedisResolver = new();
    private readonly KafkaOptionsValidator _validator;

    public KafkaOptionsValidatorTests()
    {
        _mockRedisResolver.Setup(r => r.IsRegistered(Moq.It.IsAny<string>())).Returns(true);
        _validator = new KafkaOptionsValidator(_mockRedisResolver.Object, new CompositeMessageRegistry());
    }

    private KafkaOptions CreateValidBaseOptions()
    {
        return new KafkaOptions
        {
            Servers = "localhost:9092",
            Clusters = new Dictionary<string, KafkaClusterConfiguration>
            {
                { "Default", new KafkaClusterConfiguration { BootstrapServers = "localhost:9092" } }
            }
        };
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_WhenOutboxErrorDelayIsInvalid_ShouldReturnFailure(int errorDelayMs)
    {
        var options = CreateValidBaseOptions();
        options.Outbox.IsEnabled = true;
        options.Outbox.ErrorDelayMs = errorDelayMs;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("Outbox.ErrorDelayMs", result.FailureMessage);
    }

    [Theory]
    [InlineData("bonus.events.tag-usage.changed.v1", "tags-service.bonus.tag-usage.update-references")] // 5 segments (Option 1)
    [InlineData("dev.bonus.events.tag-usage.changed.v1", "dev.tags-service.bonus.tag-usage.update-references")] // 6 segments (Option 2)
    [InlineData("bonus.events.tag-usage.changed.v1.dlq", "tags-service.bonus.tag-usage.update-references")] // 5 segments with .dlq suffix
    [InlineData("dev.bonus.events.tag-usage.changed.v1.dlq", "dev.tags-service.bonus.tag-usage.update-references")] // 6 segments with .dlq suffix
    [InlineData("bonus.events.tag-usage.changed.v1", null)] // GroupId is optional
    [InlineData("bonus.events.tag-usage.changed.v1", "")] // GroupId is empty
    public void Validate_WithValidNamingConventions_ShouldReturnSuccess(string topicName, string? groupId)
    {
        // Arrange
        var options = CreateValidBaseOptions();
        options.Topics.Add("TestEvent", new KafkaTopicConfiguration
        {
            TopicName = topicName,
            GroupId = groupId ?? string.Empty
        });

        // Act
        var result = _validator.Validate(null, options);

        // Assert
        Assert.True(result.Succeeded, result.FailureMessage);
    }

    [Theory]
    [InlineData("bonus.events.changed.v1", "tags-service.bonus.tag-usage.update-references")] // Topic has 4 segments (invalid)
    [InlineData("bonus.events.tag-usage.changed.v1.extra", "tags-service.bonus.tag-usage.update-references")] // Topic has 6 segments but GroupId has 4
    [InlineData("bonus.events.tag-usage.changed.v1", "tags-service.bonus.update-references")] // GroupId has 3 segments (invalid)
    [InlineData("bonus.events.tag-usage.changed.v1", "tags-service.bonus.tag-usage.update-references.extra")] // GroupId has 5 segments but Topic has 5
    [InlineData("dev.bonus.events.tag-usage.changed.v1", "stage.tags-service.bonus.tag-usage.update-references")] // Mismatched environment prefix
    [InlineData("bonus.events.tag-usage.changed.v1extra", "tags-service.bonus.tag-usage.update-references")] // Invalid version suffix segment
    public void Validate_WithMismatchedOrInvalidSegmentCounts_ShouldReturnFailure(string topicName, string groupId)
    {
        // Arrange
        var options = CreateValidBaseOptions();
        options.Topics.Add("TestEvent", new KafkaTopicConfiguration
        {
            TopicName = topicName,
            GroupId = groupId
        });

        // Act
        var result = _validator.Validate(null, options);

        // Assert
        Assert.True(result.Failed);
        var message = result.FailureMessage ?? string.Empty;
        Assert.True(message.Contains("segments") || message.Contains("prefix") || message.Contains("option") || message.Contains("version"));
    }

    [Theory]
    [InlineData("Bonus.events.tag-usage.changed.v1")] // Uppercase character
    [InlineData("bonus.events.tag_usage.changed.v1")] // Underscore not allowed
    [InlineData("bonus.events.tag--usage.changed.v1")] // Consecutive hyphens not allowed
    [InlineData("bonus.events.-tag-usage.changed.v1")] // Leading hyphen not allowed
    [InlineData("bonus.events.tag-usage-.changed.v1")] // Trailing hyphen not allowed
    [InlineData("bonus..tag-usage.changed.v1")] // Empty segment
    public void Validate_WithInvalidTopicNameSegments_ShouldReturnFailure(string topicName)
    {
        // Arrange
        var options = CreateValidBaseOptions();
        options.Topics.Add("TestEvent", new KafkaTopicConfiguration
        {
            TopicName = topicName,
            GroupId = "tags-service.bonus.tag-usage.update-references"
        });

        // Act
        var result = _validator.Validate(null, options);

        // Assert
        Assert.True(result.Failed);
        Assert.Contains("invalid segment", result.FailureMessage ?? string.Empty);
    }

    [Theory]
    [InlineData("Tags-service.bonus.tag-usage.update-references")] // Uppercase character
    [InlineData("tags_service.bonus.tag-usage.update-references")] // Underscore not allowed
    [InlineData("tags--service.bonus.tag-usage.update-references")] // Consecutive hyphens not allowed
    [InlineData("tags-service.bonus.tag-usage.-update-references")] // Leading hyphen
    [InlineData("tags-service.bonus.tag-usage.update-references-")] // Trailing hyphen
    public void Validate_WithInvalidGroupIdSegments_ShouldReturnFailure(string groupId)
    {
        // Arrange
        var options = CreateValidBaseOptions();
        options.Topics.Add("TestEvent", new KafkaTopicConfiguration
        {
            TopicName = "bonus.events.tag-usage.changed.v1",
            GroupId = groupId
        });

        // Act
        var result = _validator.Validate(null, options);

        // Assert
        Assert.True(result.Failed);
        Assert.Contains("invalid segment", result.FailureMessage ?? string.Empty);
    }

    [Fact]
    public void Validate_WithOffloadingEnabledAndEmptyS3Bucket_ShouldReturnFailure()
    {
        // Arrange
        var options = CreateValidBaseOptions();
        options.Topics.Add("TestEvent", new KafkaTopicConfiguration
        {
            TopicName = "bonus.events.tag-usage.changed.v1",
            EnableOffloading = true
        });
        options.Offloading.BucketName = "";
        options.Offloading.Region = "eu-central-1";

        // Act
        var result = _validator.Validate(null, options);

        // Assert
        Assert.True(result.Failed);
        Assert.Contains("BucketName", result.FailureMessage ?? string.Empty);
    }

    [Fact]
    public void Validate_WithOffloadingEnabledAndEmptyS3Region_ShouldReturnFailure()
    {
        // Arrange
        var options = CreateValidBaseOptions();
        options.Topics.Add("TestEvent", new KafkaTopicConfiguration
        {
            TopicName = "bonus.events.tag-usage.changed.v1",
            EnableOffloading = true
        });
        options.Offloading.BucketName = "my-bucket";
        options.Offloading.Region = "";

        // Act
        var result = _validator.Validate(null, options);

        // Assert
        Assert.True(result.Failed);
        Assert.Contains("Region", result.FailureMessage ?? string.Empty);
    }

    [Fact]
    public void Validate_WithCompositeAggregatorWithoutAtomicProvider_ShouldReturnFailure()
    {
        // Arrange
        var registry = new CompositeMessageRegistry();
        var builder = new CompositeMessageBuilder<TestCompositeContext>();
        builder.StartWith<TestRecoveryMessage>(enricher: (ctx, msg) => { }); // Sets IsOrdered = true
        registry.Register(builder);

        var validator = new KafkaOptionsValidator(_mockRedisResolver.Object, registry);
        var options = CreateValidBaseOptions();
        options.StagedJobs.IsEnabled = false;

        // Act
        var result = validator.Validate(null, options);

        // Assert
        Assert.True(result.Failed);
        Assert.Contains("owner-fenced atomic state transition", result.FailureMessage ?? string.Empty);
    }

    [Theory]
    [InlineData("csharpextensions.tags-service.bonus.tag-usage")]
    [InlineData("tags-service.bonus.wallet.update-references")]
    public void Validate_WithReservedWordInGroupId_ShouldReturnFailure(string reservedGroupId)
    {
        // Arrange
        var options = CreateValidBaseOptions();
        options.Topics.Add("TestEvent", new KafkaTopicConfiguration
        {
            TopicName = "bonus.events.tag-usage.changed.v1",
            GroupId = reservedGroupId
        });

        // Act
        var result = _validator.Validate(null, options);

        // Assert
        Assert.True(result.Failed);
        Assert.Contains("contains prohibited reserved words", result.FailureMessage ?? string.Empty);
    }

    [Fact]
    public void Validate_WithLongTopicNameAndSemanticMismatch_ShouldSucceedWithWarnings()
    {
        // Arrange
        var options = CreateValidBaseOptions();
        // A very long topic name (> 60 characters)
        var longTopicName = "bonus.events.tag-usage-and-extra-long-topic-name-segments-here-to-trigger-warning-limit.changed.v1";
        options.Topics.Add("ValidV5UserEventV1", new KafkaTopicConfiguration
        {
            TopicName = longTopicName,
            GroupId = "tags-service.bonus.tag-usage.update-references"
        });

        // Create validator with simulated subscriptions to trigger semantic mismatch warning
        var subscriptions = new List<MessageSubscriptionDescriptor>
        {
            new MessageSubscriptionDescriptor(
                messageType: typeof(ValidV5UserEventV1),
                handlerType: null,
                options: new KafkaSubscriptionOptions(),
                mode: SubscriptionMode.Consumer)
        };

        var validator = new KafkaOptionsValidator(
            _mockRedisResolver.Object,
            new CompositeMessageRegistry(),
            subscriptions);

        // Act
        var result = validator.Validate(null, options);

        // Assert
        Assert.True(result.Succeeded);
    }

    [Fact]
    public void Validate_WhenHmacV2KeyPathIsMissing_ShouldFailClosed()
    {
        var options = CreateValidBaseOptions();
        options.Security.SignatureWriteVersion = KafkaSignatureWriteVersion.HmacSha256V2;
        options.Security.SignatureKeyConfigurationPath = "";
        var validator = new KafkaOptionsValidator(
            _mockRedisResolver.Object,
            new CompositeMessageRegistry());

        var result = validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("configuration path", result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenHmacV2MetadataIsValid_ShouldSucceed()
    {
        var options = CreateValidBaseOptions();
        options.Security.SignatureWriteVersion = KafkaSignatureWriteVersion.HmacSha256V2;
        options.Security.SignatureKeyId = "primary-2026";
        var validator = new KafkaOptionsValidator(
            _mockRedisResolver.Object,
            new CompositeMessageRegistry());

        var result = validator.Validate(null, options);

        Assert.True(result.Succeeded, result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenSegmentingIsConfigured_FailsAtStartup()
    {
        var options = CreateValidBaseOptions();
        options.StrictTopicNaming = false;
        options.Topics["Event"] = new KafkaTopicConfiguration
        {
            TopicName = "event-topic",
            LargePayloadStrategy = LargePayloadStrategy.Segmenting
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("owner-fenced, durable reassembly protocol", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1025)]
    public void Validate_WhenProducerCacheCapacityIsInvalid_FailsAtStartup(int capacity)
    {
        var options = CreateValidBaseOptions();
        options.Producer.MaxCachedProducers = capacity;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("MaxCachedProducers", result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenOutboxConnectionNameContainsUnsafeSeparator_FailsAtStartup()
    {
        var options = CreateValidBaseOptions();
        options.Outbox.IsEnabled = true;
        options.Outbox.ConnectionStringName = "Primary;Secondary";

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("connection-string name", result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenOutboxUsesDistinctConnectionNames_Succeeds()
    {
        var options = CreateValidBaseOptions();
        options.Outbox.IsEnabled = true;
        options.Outbox.ConnectionStringName = "Primary,Secondary";

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded, result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenOutboxUsesDistinctConfigurationPaths_Succeeds()
    {
        var options = CreateValidBaseOptions();
        options.Outbox.IsEnabled = true;
        options.Outbox.ConnectionStringName =
            "Databases:OrderProcessing:Shards:0,Databases:OrderProcessing:Shards:1";

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded, result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenMessageAssemblyIsEnabledWithoutSegmenting_Succeeds()
    {
        var options = CreateValidBaseOptions();
        options.Assembly.IsEnabled = true;
        options.Assembly.Provider = AssemblyProvider.Redis;
        options.Assembly.RedisConnectionAlias = "default";

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded, result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenS3HashVerificationIsDisabled_FailsAtStartup()
    {
        var options = CreateValidBaseOptions();
        options.StrictTopicNaming = false;
        options.Topics["Event"] = new KafkaTopicConfiguration
        {
            TopicName = "event-topic",
            LargePayloadStrategy = LargePayloadStrategy.S3Offloading
        };
        options.Offloading.BucketName = "bucket";
        options.Offloading.Region = "eu-central-1";
        options.Offloading.SkipHashVerification = true;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("cannot be enabled", result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenHistoricalReplayUsesLiveGroup_FailsAtStartup()
    {
        var options = CreateValidBaseOptions();
        options.StrictTopicNaming = false;
        options.Topics[TopicAttributeResolver.Resolve(typeof(ValidV5UserEventV1))] = new KafkaTopicConfiguration
        {
            TopicName = "event-topic",
            GroupId = "live-group"
        };
        var subscriptions = new List<MessageSubscriptionDescriptor>
        {
            new(
                typeof(ValidV5UserEventV1),
                null,
                new KafkaSubscriptionOptions
                {
                    ReadMode = KafkaReadMode.HistoricalReplay,
                    ConsumerGroup = "live-group",
                    StartOffsetTime = "2026-08-01T00:00:00Z"
                },
                SubscriptionMode.Consumer)
        };
        var validator = new KafkaOptionsValidator(
            _mockRedisResolver.Object,
            new CompositeMessageRegistry(),
            subscriptions);

        var result = validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("cannot use the live consumer group", result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenRequiredDictionaryIsNull_FailsWithoutThrowing()
    {
        var options = CreateValidBaseOptions();
        options.Clusters = null!;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("null required section", result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenStrictNamingIsDisabled_StillRejectsUnsafePhysicalTopic()
    {
        var options = CreateValidBaseOptions();
        options.StrictTopicNaming = false;
        options.Topics["Unsafe"] = new KafkaTopicConfiguration
        {
            TopicName = "unsafe\ntopic",
            GroupId = "safe-group"
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("invalid physical TopicName", result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenKmsEncryptionHasNoKeyId_FailsAtStartup()
    {
        var options = CreateValidBaseOptions();
        options.StrictTopicNaming = false;
        options.Topics["Offloaded"] = new KafkaTopicConfiguration
        {
            TopicName = "offloaded-topic",
            GroupId = "offloaded-group",
            LargePayloadStrategy = LargePayloadStrategy.S3Offloading
        };
        options.Offloading.BucketName = "valid-claim-check-bucket";
        options.Offloading.Region = "eu-central-1";
        options.Offloading.ServerSideEncryption = S3ServerSideEncryptionPolicy.Kms;
        options.Offloading.KmsKeyId = string.Empty;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("KmsKeyId", result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenApplicationRetriesAreEnabledWithoutIdempotence_FailsAtStartup()
    {
        var options = CreateValidBaseOptions();
        options.Producer.EnableIdempotence = false;
        options.Producer.MaxRetryCount = 1;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("ambiguous duplicate publishes", result.FailureMessage);
    }
}

public class TestCompositeContext : ICompositeContext
{
    public string AssemblyKey { get; set; } = "";
    public bool IsReady { get; set; }
}
