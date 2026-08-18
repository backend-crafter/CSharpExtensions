namespace CSharpExtensions.Tests.Kafka;

using CSharpExtensions.Kafka.Core;
using Xunit;

public sealed class TopicAttributeResolverTests
{
    [Fact]
    public void ConventionMetadataIsResolvedWithoutInvokingMessageConstructor()
    {
        Assert.Equal(
            "EventsOrdersOrderPlacedV2",
            TopicAttributeResolver.Resolve(typeof(ThrowingConstructorEventV2)));
        Assert.Equal(
            "events.orders.order.placed.v2",
            TopicAttributeResolver.ResolveTopicName(typeof(ThrowingConstructorEventV2)));
    }

    private sealed class ThrowingConstructorEventV2
    {
        public const string MessageType = "Events";
        public const string Domain = "Orders";
        public const string Aggregate = "Order";
        public const string Action = "Placed";
        public int Version => 2;

        public ThrowingConstructorEventV2()
        {
            throw new InvalidOperationException("The message constructor must not run during metadata discovery.");
        }
    }
}
