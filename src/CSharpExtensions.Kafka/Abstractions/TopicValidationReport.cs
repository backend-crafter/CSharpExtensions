namespace CSharpExtensions.Kafka.Abstractions;

using System.Collections.Generic;

/// <summary>
/// Summary report from a topic validation scan.
/// </summary>
public sealed record TopicValidationReport(
    string TopicName,
    int TotalMessagesScanned,
    int ValidMessages,
    int InvalidMessages,
    IReadOnlyList<ValidationError> Errors);
