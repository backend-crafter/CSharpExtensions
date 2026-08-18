namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Defines a transformation mapping a historical JSON schema structure to a newer version.
/// </summary>
public interface IMessageUpcaster
{
    /// <summary>
    /// The source message schema configuration key (e.g., "SmsCampaignDispatchedV1").
    /// </summary>
    string SourceSchemaKey { get; }

    /// <summary>
    /// The target message schema configuration key (e.g., "SmsCampaignDispatchedV2").
    /// </summary>
    string TargetSchemaKey { get; }

    /// <summary>
    /// Transforms the historical JSON payload to match the target schema format.
    /// </summary>
    /// <param name="historicalJson">The historical payload string.</param>
    /// <returns>The transformed JSON payload string.</returns>
    string Upcast(string historicalJson);
}
