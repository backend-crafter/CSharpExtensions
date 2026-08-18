namespace CSharpExtensions.Tests.Kafka;

using System;
using System.Reflection;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Xunit;

public class KafkaSerializationAndTopicTests
{
    [Topic("CustomTopicKey")]
    private class EventWithTopicAttribute
    {
        public string UserId { get; set; } = string.Empty;
    }

    private class EventWithoutTopicAttribute
    {
        public string UserId { get; set; } = string.Empty;
    }

    [Fact]
    public void ResolveConfigurationKey_WithTopicAttribute_ShouldReturnConfiguredKey()
    {
        // Arrange
        var methodInfo = typeof(KafkaMessageBus).GetMethod("ResolveConfigurationKey", 
            BindingFlags.NonPublic | BindingFlags.Static);
        
        Assert.NotNull(methodInfo);

        var genericMethod = methodInfo.MakeGenericMethod(typeof(EventWithTopicAttribute));

        // Act
        var result = genericMethod.Invoke(null, null) as string;

        // Assert
        Assert.Equal("CustomTopicKey", result);
    }

    [Fact]
    public void ResolveConfigurationKey_WithoutTopicAttribute_ShouldReturnClassName()
    {
        // Arrange
        var methodInfo = typeof(KafkaMessageBus).GetMethod("ResolveConfigurationKey", 
            BindingFlags.NonPublic | BindingFlags.Static);
        
        Assert.NotNull(methodInfo);

        var genericMethod = methodInfo.MakeGenericMethod(typeof(EventWithoutTopicAttribute));

        // Act
        var result = genericMethod.Invoke(null, null) as string;

        // Assert
        Assert.Equal("EventWithoutTopicAttribute", result);
    }
}
