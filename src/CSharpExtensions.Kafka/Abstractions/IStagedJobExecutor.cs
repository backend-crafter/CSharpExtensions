using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Abstractions;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Defines the contract for executing a staged resolve job.
/// Implementations handle domain-specific verification logic (e.g., payment verification).
/// </summary>
public interface IStagedJobExecutor
{
    /// <summary>
    /// Gets the job type identifier that this executor handles.
    /// </summary>
    string JobType { get; }

    /// <summary>
    /// Executes the staged job.
    /// </summary>
    /// <param name="payloadJson">The job payload as JSON.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Result indicating success (job resolved) or failure (should retry or dead-letter).</returns>
    Task<Result> ExecuteAsync(string payloadJson, CancellationToken cancellationToken);
}
