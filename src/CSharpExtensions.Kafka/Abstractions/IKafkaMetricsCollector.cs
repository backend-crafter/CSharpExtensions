namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Provides instrumentation metrics for the event bus.
/// </summary>
public interface IKafkaMetricsCollector
{
    /// <summary>
    /// Records a published message transaction.
    /// </summary>
    /// <param name="topicName">The physical topic name.</param>
    /// <param name="byteCount">Size of the message payload in bytes.</param>
    void RecordPublish(string topicName, long byteCount);

    /// <summary>
    /// Records a message processing outcome and duration.
    /// </summary>
    /// <param name="topicName">The physical topic name.</param>
    /// <param name="isSuccess">Indicates if processing succeeded.</param>
    /// <param name="durationMilliseconds">Execution duration in milliseconds.</param>
    void RecordConsume(string topicName, bool isSuccess, long durationMilliseconds);
}
