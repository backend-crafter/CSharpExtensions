namespace CSharpExtensions.Tests.Kafka;

using System;
using System.Linq;
using CSharpExtensions.Kafka.Core;
using Moq;
using StackExchange.Redis;
using Xunit;

/// <summary>
/// Unit tests for <see cref="RedisConnectionResolver"/>.
/// The class is internal but exposed via [InternalsVisibleTo("CSharpExtensions.Tests")].
/// </summary>
public class RedisConnectionResolverTests
{
    // ──────────────────────────────────────────────────────────
    // Constructor
    // ──────────────────────────────────────────────────────────

    [Fact]
    public void Constructor_WithDefaultConnection_RegistersUnderDefaultAlias()
    {
        // Arrange
        var multiplexer = new Mock<IConnectionMultiplexer>().Object;

        // Act
        var resolver = new RedisConnectionResolver(multiplexer);

        // Assert
        Assert.True(resolver.IsRegistered("Default"));
        Assert.Same(multiplexer, resolver.Resolve("Default"));
    }

    [Fact]
    public void Constructor_WithoutDefaultConnection_DoesNotRegisterDefault()
    {
        // Arrange & Act
        var resolver = new RedisConnectionResolver();

        // Assert
        Assert.False(resolver.IsRegistered("Default"));
    }

    // ──────────────────────────────────────────────────────────
    // Register + Resolve
    // ──────────────────────────────────────────────────────────

    [Fact]
    public void Register_AndResolve_ReturnsCorrectMultiplexer()
    {
        // Arrange
        var resolver = new RedisConnectionResolver();
        var multiplexer = new Mock<IConnectionMultiplexer>().Object;

        // Act
        resolver.Register("Cache", multiplexer);
        var resolved = resolver.Resolve("Cache");

        // Assert
        Assert.Same(multiplexer, resolved);
    }

    [Fact]
    public void Register_MultipleDifferentAliases_ResolvesEachCorrectly()
    {
        // Arrange
        var resolver = new RedisConnectionResolver();
        var cacheMultiplexer = new Mock<IConnectionMultiplexer>().Object;
        var lockMultiplexer = new Mock<IConnectionMultiplexer>().Object;

        // Act
        resolver.Register("Cache", cacheMultiplexer);
        resolver.Register("Locks", lockMultiplexer);

        // Assert
        Assert.Same(cacheMultiplexer, resolver.Resolve("Cache"));
        Assert.Same(lockMultiplexer, resolver.Resolve("Locks"));
    }

    [Fact]
    public void Register_DuplicateAlias_OverwritesPreviousConnection()
    {
        // Arrange
        var resolver = new RedisConnectionResolver();
        var firstMultiplexer = new Mock<IConnectionMultiplexer>().Object;
        var secondMultiplexer = new Mock<IConnectionMultiplexer>().Object;

        // Act
        resolver.Register("Cache", firstMultiplexer);
        resolver.Register("Cache", secondMultiplexer);

        // Assert — ConcurrentDictionary indexer overwrites without throwing
        Assert.Same(secondMultiplexer, resolver.Resolve("Cache"));
    }

    // ──────────────────────────────────────────────────────────
    // Resolve — error path
    // ──────────────────────────────────────────────────────────

    [Fact]
    public void Resolve_UnregisteredAlias_ThrowsInvalidOperationException()
    {
        // Arrange
        var resolver = new RedisConnectionResolver();

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => resolver.Resolve("NonExistent"));
        Assert.Contains("NonExistent", exception.Message);
    }

    // ──────────────────────────────────────────────────────────
    // IsRegistered
    // ──────────────────────────────────────────────────────────

    [Fact]
    public void IsRegistered_RegisteredAlias_ReturnsTrue()
    {
        // Arrange
        var resolver = new RedisConnectionResolver();
        resolver.Register("Cache", new Mock<IConnectionMultiplexer>().Object);

        // Act
        var result = resolver.IsRegistered("Cache");

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void IsRegistered_UnregisteredAlias_ReturnsFalse()
    {
        // Arrange
        var resolver = new RedisConnectionResolver();

        // Act
        var result = resolver.IsRegistered("NonExistent");

        // Assert
        Assert.False(result);
    }

    // ──────────────────────────────────────────────────────────
    // GetRegisteredAliases
    // ──────────────────────────────────────────────────────────

    [Fact]
    public void GetRegisteredAliases_ReturnsAllRegisteredAliases()
    {
        // Arrange
        var defaultMultiplexer = new Mock<IConnectionMultiplexer>().Object;
        var resolver = new RedisConnectionResolver(defaultMultiplexer);
        resolver.Register("Cache", new Mock<IConnectionMultiplexer>().Object);
        resolver.Register("Locks", new Mock<IConnectionMultiplexer>().Object);

        // Act
        var aliases = resolver.GetRegisteredAliases().ToList();

        // Assert
        Assert.Contains("Default", aliases);
        Assert.Contains("Cache", aliases);
        Assert.Contains("Locks", aliases);
        Assert.Equal(3, aliases.Count);
    }

    [Fact]
    public void GetRegisteredAliases_NoRegistrations_ReturnsEmpty()
    {
        // Arrange
        var resolver = new RedisConnectionResolver();

        // Act
        var aliases = resolver.GetRegisteredAliases().ToList();

        // Assert
        Assert.Empty(aliases);
    }

    // ──────────────────────────────────────────────────────────
    // Null / empty guards
    // ──────────────────────────────────────────────────────────

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Register_NullOrEmptyAlias_ThrowsArgumentNullException(string? alias)
    {
        // Arrange
        var resolver = new RedisConnectionResolver();
        var multiplexer = new Mock<IConnectionMultiplexer>().Object;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => resolver.Register(alias!, multiplexer));
    }

    [Fact]
    public void Register_NullMultiplexer_ThrowsArgumentNullException()
    {
        // Arrange
        var resolver = new RedisConnectionResolver();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => resolver.Register("Cache", null!));
    }

    // ──────────────────────────────────────────────────────────
    // Case-insensitive alias resolution
    // (ConcurrentDictionary uses StringComparer.OrdinalIgnoreCase)
    // ──────────────────────────────────────────────────────────

    [Theory]
    [InlineData("Cache", "cache")]
    [InlineData("Cache", "CACHE")]
    [InlineData("Default", "default")]
    [InlineData("Default", "DEFAULT")]
    public void Resolve_CaseInsensitiveAlias_ReturnsCorrectMultiplexer(string registeredAlias, string resolvedAlias)
    {
        // Arrange
        var resolver = new RedisConnectionResolver();
        var multiplexer = new Mock<IConnectionMultiplexer>().Object;
        resolver.Register(registeredAlias, multiplexer);

        // Act
        var result = resolver.Resolve(resolvedAlias);

        // Assert
        Assert.Same(multiplexer, result);
    }

    [Theory]
    [InlineData("Cache", "cache")]
    [InlineData("Default", "default")]
    public void IsRegistered_CaseInsensitiveAlias_ReturnsTrue(string registeredAlias, string queriedAlias)
    {
        // Arrange
        var resolver = new RedisConnectionResolver();
        resolver.Register(registeredAlias, new Mock<IConnectionMultiplexer>().Object);

        // Act
        var result = resolver.IsRegistered(queriedAlias);

        // Assert
        Assert.True(result);
    }
}
