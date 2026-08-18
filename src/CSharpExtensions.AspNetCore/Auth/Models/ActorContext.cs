namespace CSharpExtensions.AspNetCore.Auth.Models;

/// <summary>
/// Represents the contextual identity, actor type, and role details of the current actor executing an operation.
/// </summary>
public sealed record ActorContext
{
    /// <summary>
    /// Gets the unique identifier of the actor (User ID, Employee ID, or Service ID), if available.
    /// </summary>
    public Guid? ActorId { get; init; }

    /// <summary>
    /// Gets the type of actor executing the request.
    /// </summary>
    public ActorType ActorType { get; init; } = ActorType.Anonymous;

    /// <summary>
    /// Gets the email address of the actor, if available.
    /// </summary>
    public string? Email { get; init; }

    /// <summary>
    /// Gets the display name or username of the actor, if available.
    /// </summary>
    public string? DisplayName { get; init; }

    /// <summary>
    /// Gets the role or scope assigned to the actor.
    /// </summary>
    public string? Role { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ActorContext"/> record.
    /// </summary>
    public ActorContext()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ActorContext"/> record with specified actor details.
    /// </summary>
    public ActorContext(Guid? actorId, ActorType actorType, string? email = null, string? displayName = null, string? role = null)
    {
        ActorId = actorId;
        ActorType = actorType;
        Email = email;
        DisplayName = displayName;
        Role = role;
    }

    /// <summary>
    /// Gets a value indicating whether the actor is anonymous or unauthenticated.
    /// </summary>
    public bool IsAnonymous => ActorType == ActorType.Anonymous;

    /// <summary>
    /// Gets a value indicating whether the actor is an end-user / customer.
    /// </summary>
    public bool IsUser => ActorType == ActorType.User;

    /// <summary>
    /// Gets a value indicating whether the actor is an internal employee / backoffice operator.
    /// </summary>
    public bool IsEmployee => ActorType == ActorType.Employee;

    /// <summary>
    /// Gets a value indicating whether the actor is an internal service process or S2S call.
    /// </summary>
    public bool IsService => ActorType == ActorType.Service;

    /// <summary>
    /// Gets an anonymous actor context instance.
    /// </summary>
    public static ActorContext AnonymousContext { get; } = new(null, ActorType.Anonymous);

    /// <summary>
    /// Formats a stable non-PII identity for audit logging.
    /// </summary>
    public string ToAuditString() => ActorType switch
    {
        ActorType.User => ActorId is { } userId && userId != Guid.Empty ? $"User:{userId:D}" : "User",
        ActorType.Employee => ActorId is { } employeeId && employeeId != Guid.Empty ? $"Employee:{employeeId:D}" : "Employee",
        ActorType.Service => $"S2S:{GetSafeServiceName(DisplayName)}",
        _ => "Anonymous"
    };

    /// <summary>
    /// Formats a stable non-PII identity for audit logging.
    /// </summary>
    public string ToSafeAuditString() => ToAuditString();

    /// <inheritdoc />
    public override string ToString() => ToAuditString();

    /// <summary>
    /// Creates an actor context for an end-user / client.
    /// </summary>
    public static ActorContext ForUser(Guid userId, string? email = null, string? displayName = null, string? role = null) => 
        new(userId, ActorType.User, email, displayName, role);

    /// <summary>
    /// Creates an actor context for an internal employee / backoffice operator.
    /// </summary>
    public static ActorContext ForEmployee(Guid employeeId, string? email = null, string? displayName = null, string? role = null) => 
        new(employeeId, ActorType.Employee, email, displayName, role);

    /// <summary>
    /// Creates an actor context for an internal service process or S2S call.
    /// </summary>
    public static ActorContext ForService(string? serviceName = "InternalService", string? role = "internal") => 
        new(null, ActorType.Service, null, serviceName, role);

    private static string GetSafeServiceName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "InternalService";
        }

        var source = value.AsSpan(0, Math.Min(value.Length, 64));
        Span<char> destination = stackalloc char[source.Length];
        for (var index = 0; index < source.Length; index++)
        {
            var character = source[index];
            destination[index] = char.IsAsciiLetterOrDigit(character) || character is '.' or '-' or '_'
                ? character
                : '_';
        }

        return new string(destination);
    }
}
