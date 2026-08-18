using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace CSharpExtensions.Core.Railway;

/// <summary>
/// Represents a rich error object for Railway Oriented Programming.
/// </summary>
public record Error
{
    private readonly bool _isImmutable;
    private List<string>? _details;
    private List<string>? _stackTraces;
    private Dictionary<string, object>? _metadata;

    /// <summary>
    /// Gets the default value for an empty error (None).
    /// </summary>
    public static Error None { get; } = new();

    /// <summary>
    /// Gets the immutable error used by default-initialized result values.
    /// </summary>
    public static Error Uninitialized { get; } = new(
        "Result was not initialized.",
        "Result was not initialized.",
        "UninitializedResult",
        500);

    private Error()
    {
        _isImmutable = true;
        Message = string.Empty;
        Title = string.Empty;
        Type = "None";
        Timestamp = DateTime.MinValue;
    }

    private Error(string message, string title, string type, int httpStatusCode)
    {
        _isImmutable = true;
        Message = message;
        Title = title;
        Type = type;
        HttpStatusCode = httpStatusCode;
        Timestamp = DateTime.MinValue;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Error"/> class with a message.
    /// </summary>
    /// <param name="message">The error message.</param>
    public Error(string message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);

        Message = message;
        Timestamp = DateTime.UtcNow;
        Type = "InternalServerError";
        Title = "A server error occurred.";
        HttpStatusCode = 500;
    }

    /// <summary>
    /// The descriptive error message.
    /// </summary>
    public string Message { get; init; }

    /// <summary>
    /// A short, human-readable summary of the error type.
    /// </summary>
    public string Title { get; private set; }

    /// <summary>
    /// A specific error type identifier (e.g., "UserNotFoundError").
    /// </summary>
    public string Type { get; private set; }

    /// <summary>
    /// The HTTP status code associated with this error.
    /// </summary>
    public int HttpStatusCode { get; private set; }

    /// <summary>
    /// The UTC timestamp when the error was created.
    /// </summary>
    public DateTime Timestamp { get; private set; }

    /// <summary>
    /// Additional metadata associated with the error.
    /// </summary>
    [JsonIgnore]
    public Dictionary<string, object> Metadata => _isImmutable ? [] : _metadata ??= [];

    /// <summary>
    /// Detailed list of error messages or reasons.
    /// </summary>
    [JsonIgnore]
    public List<string> Details => _isImmutable ? [] : _details ??= [];

    /// <summary>
    /// Cleaned stack traces associated with the error.
    /// </summary>
    [JsonIgnore]
    public List<string> StackTraces => _isImmutable ? [] : _stackTraces ??= [];

    /// <summary>
    /// Marks the error as a 400 Bad Request.
    /// </summary>
    public Error AsBadRequest(string type, string title)
    {
        return AsHttpStatus(400, type, title);
    }

    /// <summary>
    /// Marks the error as a 401 Unauthorized.
    /// </summary>
    public Error AsUnauthorized()
    {
        return AsHttpStatus(401, "UnauthorizedError", "Authentication failed.");
    }

    /// <summary>
    /// Marks the error as a 403 Forbidden.
    /// </summary>
    public Error AsForbidden()
    {
        return AsHttpStatus(403, "ForbiddenError", "Access denied.");
    }

    /// <summary>
    /// Marks the error as a 404 Not Found.
    /// </summary>
    public Error AsNotFound()
    {
        return AsHttpStatus(404, "NotFoundError", "The requested resource was not found.");
    }

    /// <summary>
    /// Marks the error as a 500 Internal Server Error with custom type and title.
    /// </summary>
    public Error AsInternalServer(string type, string title)
    {
        return AsHttpStatus(500, type, title);
    }

    /// <summary>
    /// Associates the error with an explicit HTTP client or server error status.
    /// </summary>
    /// <param name="statusCode">An HTTP status code in the 400-599 range.</param>
    /// <param name="type">A stable machine-readable error type.</param>
    /// <param name="title">A short human-readable error title.</param>
    public Error AsHttpStatus(int statusCode, string type, string title)
    {
        EnsureMutable();
        ArgumentOutOfRangeException.ThrowIfLessThan(statusCode, 400);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(statusCode, 599);
        ArgumentException.ThrowIfNullOrWhiteSpace(type);
        ArgumentException.ThrowIfNullOrWhiteSpace(title);

        Type = type;
        Title = title;
        HttpStatusCode = statusCode;
        return this;
    }

    /// <summary>
    /// Adds a detail message to the error.
    /// </summary>
    public Error WithDetails(string details)
    {
        EnsureMutable();
        if (!string.IsNullOrWhiteSpace(details) && !Details.Contains(details) && Message != details)
        {
            Details.Add(details);
        }
        return this;
    }

    /// <summary>
    /// Adds metadata to the error.
    /// </summary>
    public Error WithMetadata(string key, object value)
    {
        EnsureMutable();
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);

        Metadata[key] = value;
        return this;
    }

    /// <summary>
    /// Adds a collection of metadata to the error.
    /// </summary>
    public Error WithMetadata(IDictionary<string, object> metadata)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(metadata);

        foreach (var (key, value) in metadata)
        {
            WithMetadata(key, value);
        }
        return this;
    }

    /// <summary>
    /// Captures exception details into the error, including messages and cleaned stack trace.
    /// </summary>
    public Error CausedBy(Exception? exception)
    {
        EnsureMutable();
        if (exception == null) return this;

        // Exception messages and stack traces may contain PII, credentials, SQL,
        // or downstream response bodies. Keep only the stable diagnostic type.
        Metadata["exception_type"] = exception.GetType().FullName ?? exception.GetType().Name;

        return this;
    }

    /// <summary>
    /// Gets the associated HTTP status code.
    /// </summary>
    public int GetHttpStatusCode() => HttpStatusCode;

    /// <summary>
    /// Sets a custom timestamp.
    /// </summary>
    public Error WithTimestamp(DateTime timestamp)
    {
        EnsureMutable();
        Timestamp = timestamp;
        return this;
    }

    /// <summary>
    /// Logs the error using the global static logger.
    /// </summary>
    public void Log()
    {
        Log(RailwayDiagnostics.Logger);
    }

    /// <summary>
    /// Logs the error using structured logging.
    /// </summary>
    public void Log(ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        logger.LogError(
            "Operation failed. ErrorType={ErrorType} StatusCode={StatusCode} Timestamp={Timestamp}",
            Type,
            HttpStatusCode,
            Timestamp);
    }

    /// <summary>
    /// Logs the error using the global static logger and returns the error itself,
    /// enabling fluent chains and implicit conversion to failed Results.
    /// </summary>
    public Error LogBeforeReturn()
    {
        Log();
        return this;
    }

    /// <summary>
    /// Logs the error using structured logging and returns the error itself,
    /// enabling fluent chains and implicit conversion to failed Results.
    /// </summary>
    public Error LogBeforeReturn(ILogger logger)
    {
        Log(logger);
        return this;
    }

    /// <summary>
    /// Checks if the error has a metadata entry with the specified key.
    /// </summary>
    public bool HasMetadataKey(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return _metadata?.ContainsKey(key) ?? false;
    }

    /// <summary>
    /// Checks if the error has a metadata entry with the specified key and matches the predicate.
    /// </summary>
    public bool HasMetadata(string key, Func<object, bool> predicate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(predicate);

        return _metadata != null && _metadata.TryGetValue(key, out var value) && predicate(value);
    }

    /// <inheritdoc />
    public virtual bool Equals(Error? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        if (GetType() != other.GetType()) return false;

        return Message == other.Message &&
               Type == other.Type &&
               Title == other.Title &&
               HttpStatusCode == other.HttpStatusCode &&
               DetailsEqual(_details, other._details) &&
               MetadataEquals(_metadata, other._metadata);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Message);
        hash.Add(Type);
        hash.Add(Title);
        hash.Add(HttpStatusCode);
        
        if (_details != null)
        {
            foreach (var detail in _details) hash.Add(detail);
        }

        if (_metadata is { Count: > 0 })
        {
            var metadataHash = 0;
            foreach (var kvp in _metadata)
            {
                metadataHash ^= HashCode.Combine(
                    StringComparer.Ordinal.GetHashCode(kvp.Key),
                    kvp.Value?.GetHashCode() ?? 0);
            }

            hash.Add(metadataHash);
        }

        return hash.ToHashCode();
    }

    /// <summary>
    /// Prints the members of the error record, formatting Metadata, Details, and StackTraces as readable collections instead of their type names.
    /// </summary>
    /// <param name="builder">The string builder to append to.</param>
    /// <returns>True if the members were successfully printed; otherwise, false.</returns>
    protected virtual bool PrintMembers(System.Text.StringBuilder builder)
    {
        builder.Append($"{nameof(Title)} = {Title}, ");
        builder.Append($"{nameof(Type)} = {Type}, ");
        builder.Append($"{nameof(HttpStatusCode)} = {HttpStatusCode}, ");
        builder.Append($"{nameof(Timestamp)} = {Timestamp:O}");

        return true;
    }

    private void EnsureMutable()
    {
        if (_isImmutable)
        {
            throw new InvalidOperationException("Sentinel errors are immutable.");
        }
    }

    private static bool MetadataEquals(
        IReadOnlyDictionary<string, object>? left,
        IReadOnlyDictionary<string, object>? right)
    {
        if (left is null || left.Count == 0)
        {
            return right is null || right.Count == 0;
        }

        if (right is null || right.Count == 0)
        {
            return false;
        }

        if (left.Count != right.Count)
        {
            return false;
        }

        foreach (var (key, value) in left)
        {
            if (!right.TryGetValue(key, out var otherValue) || !Equals(value, otherValue))
            {
                return false;
            }
        }

        return true;
    }

    private static bool DetailsEqual(
        IReadOnlyList<string>? left,
        IReadOnlyList<string>? right)
    {
        if (left is null || left.Count == 0)
        {
            return right is null || right.Count == 0;
        }

        return right is not null && left.SequenceEqual(right);
    }
}
