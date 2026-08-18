namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Configures the behavior and target destination of the Dead Letter Queue for a specific subscriber.
/// </summary>
public sealed record DlqConfigurationOptions(
    bool IsEnabled = false,
    string TargetDlqTopic = "",
    int MaxDeliveryAttempts = 5);
