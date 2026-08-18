using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// High-speed distributed duplicate detector.
/// </summary>
public interface IDistributedDuplicateDetector
{
    /// <summary>
    /// Atomically attempts to acquire the message processing claim.
    /// </summary>
    /// <param name="messageId">The unique message identifier.</param>
    /// <param name="consumerGroup">The consumer group scope.</param>
    /// <param name="retentionSeconds">TTL window for storing the uniqueness claim.</param>
    /// <param name="ownerToken">The token identifying the owner of this processing attempt.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>
    /// A successful result containing <see langword="true"/> when the claim is acquired or
    /// <see langword="false"/> only when processing was previously completed. An active claim
    /// owned by another processor is returned as a failure and must never be treated as a duplicate.
    /// </returns>
    Task<Result<bool>> TryClaimUniqueAsync(
        string messageId,
        string consumerGroup,
        int retentionSeconds,
        CancellationToken cancellationToken = default,
        string? ownerToken = null);

    /// <summary>
    /// Completes the idempotency claim, shifting the status from 'Processing' to 'Completed'.
    /// </summary>
    /// <param name="messageId">The unique message identifier.</param>
    /// <param name="consumerGroup">The consumer group scope.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="retentionSeconds">TTL window for storing the uniqueness claim.</param>
    /// <param name="ownerToken">The token identifying the owner of this processing attempt.</param>
    /// <returns>A functional Result indicating execution outcome.</returns>
    Task<Result> CompleteClaimAsync(
        string messageId,
        string consumerGroup,
        CancellationToken cancellationToken = default,
        int retentionSeconds = 604800,
        string? ownerToken = null);

    /// <summary>
    /// Releases the idempotency claim, removing the 'Processing' status to allow immediate re-processing.
    /// </summary>
    /// <param name="messageId">The unique message identifier.</param>
    /// <param name="consumerGroup">The consumer group scope.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="ownerToken">The token identifying the owner of this processing attempt.</param>
    /// <returns>A functional Result indicating execution outcome.</returns>
    Task<Result> ReleaseClaimAsync(
        string messageId,
        string consumerGroup,
        CancellationToken cancellationToken = default,
        string? ownerToken = null);
}

/// <summary>
/// Optional capability for extending an active owner-fenced processing claim.
/// </summary>
public interface IDistributedDuplicateClaimRenewer
{
    /// <summary>
    /// Extends the claim only when it is still owned by <paramref name="ownerToken"/>.
    /// </summary>
    Task<Result> RenewClaimAsync(
        string messageId,
        string consumerGroup,
        string ownerToken,
        CancellationToken cancellationToken = default);
}
