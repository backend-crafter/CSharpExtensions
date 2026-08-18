using CSharpExtensions.Kafka.Core;

namespace CSharpExtensions.Tests.Kafka;

using CSharpExtensions.Kafka.Evolution;
using Xunit;

public sealed class MessageVersionResolverTests
{
    [Fact]
    public void GenericResolverReadsConstVersionWithoutCreatingMessage()
    {
        Assert.Equal(7, MessageVersionResolver.GetMessageVersion<ConstVersionMessage>());
    }

    [Fact]
    public void InstanceResolverReadsConstVersion()
    {
        Assert.Equal(7, MessageVersionResolver.GetMessageVersion(new ConstVersionMessage("value")));
    }

    [Fact]
    public void GenericResolverReadsStaticVersionWithoutCreatingMessage()
    {
        Assert.Equal(3, MessageVersionResolver.GetMessageVersion<StaticVersionMessage>());
    }

    [Fact]
    public void ExistingInstancePropertyBehaviorIsPreserved()
    {
        Assert.Equal(2, MessageVersionResolver.GetMessageVersion(new InstanceVersionMessage()));
    }

    [Fact]
    public void TypeResolverReadsInstanceVersionWithoutInvokingConstructor()
    {
        Assert.Equal(4, MessageVersionResolver.GetMessageVersion(typeof(ThrowingConstructorMessage)));
        Assert.Equal(4, MessageVersionResolver.GetMessageVersion<ThrowingConstructorMessage>());
    }

    [Theory]
    [InlineData("Event.v0")]
    [InlineData("Event.v1001")]
    [InlineData("EventV999999999999999999999999")]
    public void ExplicitInvalidSchemaSuffixFailsClosed(string schemaKey)
    {
        Assert.False(MessageVersionResolver.TryResolveSourceSchemaKey(schemaKey, "{}", out _));
    }

    [Theory]
    [InlineData("{\"version\":0}")]
    [InlineData("{\"version\":1001}")]
    [InlineData("{\"version\":\"2\"}")]
    [InlineData("{\"schemaVersion\":\"1..0\"}")]
    [InlineData("{\"schemaVersion\":false}")]
    public void ExplicitInvalidPayloadVersionFailsClosed(string payload)
    {
        Assert.False(MessageVersionResolver.TryResolveSourceSchemaKey("Event", payload, out _));
    }

    [Fact]
    public void MissingPayloadVersionPreservesLegacyVersionOne()
    {
        Assert.True(MessageVersionResolver.TryResolveSourceSchemaKey("Event", "{}", out var schemaKey));
        Assert.Equal("Event.v1", schemaKey);
    }

    [Fact]
    public void VersionMarkerInsideSchemaNameIsNotTreatedAsSuffix()
    {
        Assert.Equal(
            "Event.vArchive.v2",
            MessageVersionResolver.ResolveSourceSchemaKey("Event.vArchive", "{\"version\":2}"));
    }

    private sealed record ConstVersionMessage(string Value)
    {
        public const int Version = 7;
    }

    private sealed class StaticVersionMessage
    {
        public static int Version => 3;

        public StaticVersionMessage()
        {
            throw new InvalidOperationException("The static version path must not construct the message.");
        }
    }

    private sealed class InstanceVersionMessage
    {
        public int Version => 2;
    }

    private sealed class ThrowingConstructorMessage
    {
        public int Version => 4;

        public ThrowingConstructorMessage()
        {
            throw new InvalidOperationException("The message constructor must not run during metadata discovery.");
        }
    }
}
