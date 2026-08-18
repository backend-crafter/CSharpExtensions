using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Abstractions;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Defines the contract for a message handler in the railway-oriented programming style.
/// </summary>
/// <typeparam name="TMessage">The message type.</typeparam>
public interface IMessageHandler<in TMessage> where TMessage : class
{
    /// <summary>
    /// Processes the incoming message asynchronously.
    /// </summary>
    /// <param name="message">The message instance to handle.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A functional Result indicating execution outcome.</returns>
    Task<Result> HandleAsync(TMessage message, CancellationToken cancellationToken);
}
