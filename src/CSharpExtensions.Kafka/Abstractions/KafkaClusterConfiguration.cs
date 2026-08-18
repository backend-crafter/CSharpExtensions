namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Holds connection and authentication options for a specific physical Kafka cluster.
/// </summary>
public sealed class KafkaClusterConfiguration
{
    /// <summary>
    /// Comma-separated list of broker host:port pairs.
    /// </summary>
    public string BootstrapServers { get; set; } = "";

    /// <summary>
    /// Security protocol for broker communication.
    /// Valid values: Plaintext, Ssl, SaslPlaintext, SaslSsl.
    /// </summary>
    public string SecurityProtocol { get; set; } = "";

    /// <summary>
    /// SASL authentication mechanism.
    /// Valid values: Gssapi, Plain, ScramSha256, ScramSha512, OAuthBearer.
    /// </summary>
    public string SaslMechanism { get; set; } = "";

    /// <summary>
    /// Cluster-level SASL username. Used as a fallback when topic-level credentials are not configured.
    /// </summary>
    public string SaslUsername { get; set; } = "";

    /// <summary>
    /// Cluster-level SASL password. Used as a fallback when topic-level credentials are not configured.
    /// </summary>
    public string SaslPassword { get; set; } = "";

    /// <summary>
    /// Initial backoff time in milliseconds for reconnection attempts.
    /// </summary>
    public int ReconnectBackoffMs { get; set; } = 100;

    /// <summary>
    /// Maximum backoff time in milliseconds for reconnection attempts.
    /// </summary>
    public int ReconnectBackoffMaxMs { get; set; } = 10000;
}
