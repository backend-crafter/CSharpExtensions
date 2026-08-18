namespace CSharpExtensions.Kafka.Evolution;

/// <summary>
/// Defines a contract to automatically detect the schema key of a message from its raw JSON payload.
/// This acts as a fallback mechanism when headers indicating the schema version are missing or invalid.
/// </summary>
public interface ISchemaDetector
{
    /// <summary>
    /// The target schema key that this detector relates to (e.g., "EligibleWagerFactRecordedDto").
    /// </summary>
    string TargetSchemaKey { get; }

    /// <summary>
    /// Checks whether the raw JSON payload conforms to the target schema.
    /// </summary>
    /// <param name="rawPayloadJson">The JSON payload to check.</param>
    /// <returns>True if it matches the target schema; otherwise, false.</returns>
    bool IsTargetSchema(string rawPayloadJson);

    /// <summary>
    /// Analyzes the raw JSON payload to determine which historical schema key it matches.
    /// </summary>
    /// <param name="rawPayloadJson">The JSON payload to analyze.</param>
    /// <returns>The matched historical schema key, or null if no match is found.</returns>
    string? DetectSourceSchema(string rawPayloadJson);
}
