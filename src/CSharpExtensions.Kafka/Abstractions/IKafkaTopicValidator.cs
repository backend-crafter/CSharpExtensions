using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Abstractions;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Validates the structural integrity of messages in a Kafka topic.
/// </summary>
public interface IKafkaTopicValidator
{
    /// <summary>
    /// Scans a topic and validates message structure against expected schema.
    /// </summary>
    /// <param name="topicConfigurationKey">The topic configuration key matching a Kafka:Topics entry.</param>
    /// <param name="maxMessages">Maximum number of messages to scan. Default 1000.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Result containing the validation report.</returns>
    Task<Result<TopicValidationReport>> ValidateAsync(
        string topicConfigurationKey,
        int maxMessages = 1000,
        CancellationToken cancellationToken = default);
}
