namespace CSharpExtensions.Kafka.Core;

using System.Diagnostics;

/// <summary>
/// Provides a centralized <see cref="ActivitySource"/> for telemetry tracing across the Kafka library.
/// </summary>
internal static class KafkaDiagnostics
{
    /// <summary>
    /// The shared ActivitySource name.
    /// </summary>
    public const string ActivitySourceName = "CSharpExtensions.Kafka";

    /// <summary>
    /// Centralized ActivitySource instance.
    /// </summary>
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
