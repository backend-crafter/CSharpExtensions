namespace CSharpExtensions.Kafka.Abstractions;

using System.Collections.Generic;
using System.Threading;

/// <summary>
/// Provides handler-less message consumption from Kafka topics.
/// Registered automatically when <c>Subscribe&lt;TMessage&gt;</c> is called without <c>AddHandler</c>.
/// Inject this interface into a BackgroundService for manual message processing.
/// </summary>
/// <typeparam name="TMessage">The message type.</typeparam>
public interface IKafkaConsumer<TMessage> where TMessage : class
{
    /// <summary>
    /// Asynchronously enumerates messages from the subscribed Kafka topic.
    /// Each message must be explicitly acknowledged or rejected via <see cref="ConsumeContext{TMessage}"/>.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An async enumerable of consume contexts.</returns>
    IAsyncEnumerable<ConsumeContext<TMessage>> ConsumeAsync(CancellationToken cancellationToken);
}
