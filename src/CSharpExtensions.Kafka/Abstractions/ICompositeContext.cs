namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Defines the contract for composite contexts in stateful aggregators/sagas.
/// </summary>
public interface ICompositeContext
{
    /// <summary>
    /// Gets or sets the assembly key identifying this unique aggregator instance.
    /// </summary>
    string AssemblyKey { get; set; }

    /// <summary>
    /// Gets a value indicating whether all expected parts of the composite context have been enriched and are ready.
    /// </summary>
    bool IsReady { get; }
}
