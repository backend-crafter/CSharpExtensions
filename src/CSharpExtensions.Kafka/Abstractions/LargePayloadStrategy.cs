namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Defines the strategy used to handle large message payloads.
/// </summary>
public enum LargePayloadStrategy
{
    /// <summary>
    /// Offloads large payloads to AWS S3 (Claim Check pattern).
    /// </summary>
    S3Offloading,

    /// <summary>
    /// Splits large payloads into sequential segments published as individual messages.
    /// </summary>
    Segmenting
}
