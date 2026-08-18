using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Abstractions;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Defines the high-level Message Bus abstraction for publishing messages to Kafka.
/// Subscribe operations are handled declaratively via the builder at registration time.
/// </summary>
public interface IMessageBus
{
    /// <summary>
    /// Publishes a single message to its configured topic.
    /// </summary>
    /// <typeparam name="TMessage">The type of the message.</typeparam>
    /// <param name="message">The message instance.</param>
    /// <param name="messageKey">Optional message key used for partitioning.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A railway-oriented Result indicating success or error.</returns>
    Task<Result> PublishAsync<TMessage>(
        TMessage message,
        string? messageKey = null,
        CancellationToken cancellationToken = default) where TMessage : class;
}
