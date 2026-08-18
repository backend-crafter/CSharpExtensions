using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;

namespace CSharpExtensions.Tests.Kafka;

using System;
using System.Collections.Generic;
using CSharpExtensions.Kafka.Evolution;
using Moq;
using Xunit;

public sealed class MessageUpcastRegistryTests
{
    [Fact]
    public void UpcastMessage_WhenSourceAndTargetKeysAreIdentical_ReturnsOriginalPayload()
    {
        // Arrange
        var upcasters = new List<IMessageUpcaster>();
        var registry = new MessageUpcastRegistry(upcasters);
        var originalPayload = "{\"message\":\"hello\"}";

        // Act
        var result = registry.UpcastMessage(originalPayload, "TestMessageV1", "TestMessageV1");

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(originalPayload, result.Value);
    }

    [Fact]
    public void UpcastMessage_WhenDirectUpcasterExists_TransformsPayloadSuccessfully()
    {
        // Arrange
        var mockUpcaster = new Mock<IMessageUpcaster>();
        mockUpcaster.Setup(u => u.SourceSchemaKey).Returns("TestMessageV1");
        mockUpcaster.Setup(u => u.TargetSchemaKey).Returns("TestMessageV2");
        mockUpcaster.Setup(u => u.Upcast(It.IsAny<string>())).Returns("{\"version\":2}");

        var upcasters = new List<IMessageUpcaster> { mockUpcaster.Object };
        var registry = new MessageUpcastRegistry(upcasters);

        // Act
        var result = registry.UpcastMessage("{\"version\":1}", "TestMessageV1", "TestMessageV2");

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal("{\"version\":2}", result.Value);
        mockUpcaster.Verify(u => u.Upcast("{\"version\":1}"), Times.Once);
    }

    [Fact]
    public void UpcastMessage_WhenMultiStepUpcastPathExists_AppliesAllTransformsSequentially()
    {
        // Arrange
        var upcaster1 = new Mock<IMessageUpcaster>();
        upcaster1.Setup(u => u.SourceSchemaKey).Returns("V1");
        upcaster1.Setup(u => u.TargetSchemaKey).Returns("V2");
        upcaster1.Setup(u => u.Upcast("v1_data")).Returns("v2_data");

        var upcaster2 = new Mock<IMessageUpcaster>();
        upcaster2.Setup(u => u.SourceSchemaKey).Returns("V2");
        upcaster2.Setup(u => u.TargetSchemaKey).Returns("V3");
        upcaster2.Setup(u => u.Upcast("v2_data")).Returns("v3_data");

        var upcasters = new List<IMessageUpcaster> { upcaster1.Object, upcaster2.Object };
        var registry = new MessageUpcastRegistry(upcasters);

        // Act
        var result = registry.UpcastMessage("v1_data", "V1", "V3");

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal("v3_data", result.Value);
        upcaster1.Verify(u => u.Upcast("v1_data"), Times.Once);
        upcaster2.Verify(u => u.Upcast("v2_data"), Times.Once);
    }

    [Fact]
    public void UpcastMessage_WhenNoUpcastPathExists_ReturnsFailureResult()
    {
        // Arrange
        var mockUpcaster = new Mock<IMessageUpcaster>();
        mockUpcaster.Setup(u => u.SourceSchemaKey).Returns("V1");
        mockUpcaster.Setup(u => u.TargetSchemaKey).Returns("V2");

        var upcasters = new List<IMessageUpcaster> { mockUpcaster.Object };
        var registry = new MessageUpcastRegistry(upcasters);

        // Act
        var result = registry.UpcastMessage("v1_data", "V1", "V3");

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Contains("No Kafka upcaster path resolved", result.Error.Message);
    }
}
