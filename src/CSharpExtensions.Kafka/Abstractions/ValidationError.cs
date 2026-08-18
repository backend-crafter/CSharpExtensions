namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Details of a single validation error found during scanning.
/// </summary>
public sealed record ValidationError(
    long Offset,
    int Partition,
    string ErrorCategory,
    string ErrorMessage);
