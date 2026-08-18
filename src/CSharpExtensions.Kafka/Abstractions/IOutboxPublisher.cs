using System.Data;
using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Enqueues messages to the local database outbox table in scope of a database transaction.
/// </summary>
public interface IOutboxPublisher
{
    /// <summary>
    /// Saves the message to the local database outbox.
    /// </summary>
    /// <typeparam name="TMessage">The type of the message.</typeparam>
    /// <param name="message">The message instance to enqueue.</param>
    /// <param name="dbTransaction">The database transaction scope.</param>
    /// <param name="messageKey">Optional partitioning key.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Result indicating if the database insert succeeded.</returns>
    Task<Result> EnqueueAsync<TMessage>(
        TMessage message,
        IDbTransaction dbTransaction,
        string? messageKey = null,
        CancellationToken cancellationToken = default) where TMessage : class;
}
