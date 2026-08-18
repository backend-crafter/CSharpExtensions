using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Abstractions;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Handles the assembly of multi-segment Kafka messages.
/// Multi-segment messages are split across multiple Kafka records and must be
/// reassembled before processing.
/// </summary>
public interface IMessageAssembler
{
    /// <summary>
    /// Attempts to assemble a complete message from a received segment.
    /// </summary>
    /// <param name="segmentPayload">The payload of this segment.</param>
    /// <param name="assemblyKey">The unique key grouping segments of the same logical message.</param>
    /// <param name="segmentIndex">Zero-based index of this segment.</param>
    /// <param name="totalSegments">Total number of segments in the logical message.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>
    /// A Result containing the assembled payload when all segments are received,
    /// or null when more segments are still pending.
    /// </returns>
    Task<Result<string?>> TryAssembleAsync(
        string segmentPayload,
        string assemblyKey,
        int segmentIndex,
        int totalSegments,
        CancellationToken cancellationToken);
}
