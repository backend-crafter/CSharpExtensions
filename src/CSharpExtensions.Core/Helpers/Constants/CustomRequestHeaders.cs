namespace CSharpExtensions.Core.Helpers.Constants;

/// <summary>
/// Centralized standard HTTP and messaging headers used across services.
/// All header names follow the modern RFC 9113 / RFC 9114 lowercase standard.
/// </summary>
public static class CustomRequestHeaders
{
    // ==========================================
    // Tracing & Request Metadata
    // ==========================================

    /// <summary>
    /// Unique identifier for the incoming HTTP request.
    /// </summary>
    public const string RequestId = "x-request-id";

    /// <summary>
    /// Unique identifier for tracing requests across distributed services.
    /// </summary>
    public const string CorrelationId = "x-correlation-id";

    /// <summary>
    /// Distributed tracing trace identifier.
    /// </summary>
    public const string TraceId = "x-trace-id";

    /// <summary>
    /// Requested API version header.
    /// </summary>
    public const string ApiVersion = "x-api-version";

    // ==========================================
    // Actor Context (Identity Propagation)
    // ==========================================

    /// <summary>
    /// Type of the authenticated actor (e.g. User, Employee, Service).
    /// </summary>
    public const string ActorType = "x-actor-type";

    /// <summary>
    /// Primary identifier of the authenticated actor.
    /// </summary>
    public const string ActorId = "x-actor-id";

    /// <summary>
    /// Role of the authenticated actor.
    /// </summary>
    public const string ActorRole = "x-actor-role";

    /// <summary>
    /// Explicit user identifier for end-user clients.
    /// </summary>
    public const string UserId = "x-user-id";

    /// <summary>
    /// Explicit employee identifier for internal staff / backoffice operators.
    /// </summary>
    public const string EmployeeId = "x-employee-id";

    /// <summary>
    /// Email of the authenticated user or employee.
    /// </summary>
    public const string UserEmail = "x-user-email";

    /// <summary>
    /// Display name of the authenticated user or employee.
    /// </summary>
    public const string UserName = "x-user-name";

    /// <summary>
    /// Name of the calling internal service.
    /// </summary>
    public const string ServiceName = "x-service-name";

    // ==========================================
    // Service-to-Service (S2S) Authentication
    // ==========================================

    /// <summary>
    /// Canonical Service-to-Service static authentication token header.
    /// </summary>
    public const string S2SToken = "x-s2s-token";

    /// <summary>
    /// Short alias header for S2S token.
    /// </summary>
    public const string S2S = "x-s2s";

    /// <summary>
    /// Legacy alias header for internal API key.
    /// </summary>
    public const string InternalApiKey = "x-internal-api-key";

    // ==========================================
    // Messaging & Event-Driven (Kafka)
    // ==========================================

    /// <summary>
    /// Unique identifier for messaging system message.
    /// </summary>
    public const string MessageId = "x-message-id";

    /// <summary>
    /// Cryptographic signature for verifying message origin.
    /// </summary>
    public const string MessageSignature = "x-message-signature";

    /// <summary>
    /// Identifies the CLR type / schema of the event payload.
    /// </summary>
    public const string EventSchemaVersion = "x-event-schema-version";

    /// <summary>
    /// The original topic prior to routing to the Dead Letter Queue.
    /// </summary>
    public const string OriginalTopic = "x-original-topic";

    /// <summary>
    /// The original partition prior to routing to the Dead Letter Queue.
    /// </summary>
    public const string OriginalPartition = "x-original-partition";

    /// <summary>
    /// The original offset prior to routing to the Dead Letter Queue.
    /// </summary>
    public const string OriginalOffset = "x-original-offset";

    /// <summary>
    /// The exception type that caused routing to the Dead Letter Queue.
    /// </summary>
    public const string ExceptionType = "x-exception-type";

    /// <summary>
    /// The exception message that caused routing to the Dead Letter Queue.
    /// </summary>
    public const string ExceptionMessage = "x-exception-message";

    /// <summary>
    /// The exception stack trace that caused routing to the Dead Letter Queue.
    /// </summary>
    public const string ExceptionStackTrace = "x-exception-stacktrace";

    /// <summary>
    /// The timestamp when the message processing failed.
    /// </summary>
    public const string FailedAtUtc = "x-failed-at-utc";

    /// <summary>
    /// Assembly key header for multi-segment message assembly.
    /// </summary>
    public const string AssemblyKey = "x-assembly-key";

    /// <summary>
    /// Segment index header for multi-segment message assembly.
    /// </summary>
    public const string SegmentIndex = "x-segment-index";

    /// <summary>
    /// Total segments header for multi-segment message assembly.
    /// </summary>
    public const string TotalSegments = "x-total-segments";
}
