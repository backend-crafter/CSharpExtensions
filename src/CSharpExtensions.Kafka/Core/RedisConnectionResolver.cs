namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using CSharpExtensions.Kafka.Abstractions;
using StackExchange.Redis;

/// <summary>
/// Internal implementation of <see cref="IRedisConnectionResolver"/> supporting named Redis connections.
/// Backward-compatible: if a single <see cref="IConnectionMultiplexer"/> is registered in DI,
/// it is automatically available under the "Default" alias.
/// </summary>
internal sealed class RedisConnectionResolver : IRedisConnectionResolver
{
    private readonly ConcurrentDictionary<string, IConnectionMultiplexer> _connections = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisConnectionResolver"/> class.
    /// If a default <see cref="IConnectionMultiplexer"/> is provided, it is registered under "Default".
    /// </summary>
    /// <param name="defaultConnection">Optional default connection multiplexer from DI.</param>
    public RedisConnectionResolver(IConnectionMultiplexer? defaultConnection = null)
    {
        if (defaultConnection is not null)
        {
            _connections["Default"] = defaultConnection;
        }
    }

    /// <summary>
    /// Registers a named Redis connection.
    /// </summary>
    /// <param name="connectionAlias">The alias for the connection.</param>
    /// <param name="connectionMultiplexer">The connection multiplexer instance.</param>
    /// <exception cref="ArgumentNullException">Thrown when the alias or multiplexer is null.</exception>
    public void Register(string connectionAlias, IConnectionMultiplexer connectionMultiplexer)
    {
        if (string.IsNullOrWhiteSpace(connectionAlias))
            throw new ArgumentNullException(nameof(connectionAlias));
        if (connectionMultiplexer is null)
            throw new ArgumentNullException(nameof(connectionMultiplexer));

        _connections[connectionAlias] = connectionMultiplexer;
    }

    /// <summary>
    /// Gets all registered connection aliases.
    /// </summary>
    /// <returns>An enumerable of registered alias names.</returns>
    public IEnumerable<string> GetRegisteredAliases()
    {
        return _connections.Keys;
    }

    /// <inheritdoc />
    public IConnectionMultiplexer Resolve(string connectionAlias = "Default")
    {
        if (_connections.TryGetValue(connectionAlias, out var connection))
        {
            return connection;
        }

        throw new InvalidOperationException(
            $"Redis connection alias '{connectionAlias}' is not registered. " +
            $"Register it via KafkaBuilder.AddRedisConnection(\"{connectionAlias}\", connectionMultiplexer) " +
            $"or ensure an IConnectionMultiplexer is available in the DI container.");
    }

    /// <inheritdoc />
    public bool IsRegistered(string connectionAlias = "Default")
    {
        return _connections.ContainsKey(connectionAlias);
    }
}
