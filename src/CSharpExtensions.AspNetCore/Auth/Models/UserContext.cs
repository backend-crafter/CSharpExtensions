namespace CSharpExtensions.AspNetCore.Auth.Models;

/// <summary>
/// Represents the user context of an authenticated user.
/// </summary>
public sealed record UserContext
{
    public Guid? UserId { get; init; }
    public string Username { get; init; } = string.Empty;
    public string? Email { get; init; }
    public string? Role { get; init; }

    public UserContext() { }

    public UserContext(string username, Guid? userId = null, string? email = null, string? role = null)
    {
        Username = username;
        UserId = userId;
        Email = email;
        Role = role;
    }

    /// <summary>
    /// Formats a stable non-PII identity for audit logging.
    /// </summary>
    public string ToAuditString()
        => UserId is { } userId && userId != Guid.Empty ? $"User:{userId:D}" : "User";

    public static implicit operator string(UserContext context) => context?.ToAuditString() ?? string.Empty;

    public override string ToString() => ToAuditString();
}
