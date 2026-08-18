namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// The assembly provider type.
/// </summary>
public enum AssemblyProvider
{
    /// <summary>Redis Hash-based assembly (high performance, in-memory).</summary>
    Redis,

    /// <summary>SQL Server-based assembly (durable, with DDL auto-provisioning).</summary>
    SqlServer
}
