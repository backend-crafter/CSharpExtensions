namespace CSharpExtensions.Kafka.Abstractions;

using StackExchange.Redis;

/// <summary>
/// Resolves named Redis connections for multi-instance support.
/// Enables different Kafka features (deduplication, message assembly, distributed locks)
/// to use separate Redis instances or connection pools.
/// </summary>
/// <remarks>
/// The default alias "Default" is automatically resolved when an <see cref="IConnectionMultiplexer"/>
/// is registered in the DI container. For multi-instance support, register connections via
/// <c>KafkaBuilder.AddRedisConnection(alias, multiplexer)</c>.
/// </remarks>
public interface IRedisConnectionResolver
{
    /// <summary>
    /// Resolves a named Redis connection.
    /// </summary>
    /// <param name="connectionAlias">The connection alias. Defaults to "Default".</param>
    /// <returns>The resolved connection multiplexer.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the specified alias is not registered.</exception>
    IConnectionMultiplexer Resolve(string connectionAlias = "Default");

    /// <summary>
    /// Checks whether a connection with the specified alias is registered.
    /// </summary>
    /// <param name="connectionAlias">The connection alias to check.</param>
    /// <returns>True if the connection alias exists, false otherwise.</returns>
    bool IsRegistered(string connectionAlias = "Default");
}
