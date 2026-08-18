namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Controls the behavior of the upcaster chain generator.
/// </summary>
public enum UpcasterGenerationMode
{
    /// <summary>
    /// Checks for the existence of the resolver file. If it exists, skip schema analysis and generation.
    /// </summary>
    OnlyOnce,

    /// <summary>
    /// Always executes the schema analysis and re-generates files on every build.
    /// </summary>
    Repeatable
}
