using System.Security.Claims;
using CSharpExtensions.AspNetCore.Auth.Models;
using CSharpExtensions.AspNetCore.Auth.Options;
using CSharpExtensions.Core.Helpers.Constants;
using Microsoft.AspNetCore.Http;

namespace CSharpExtensions.AspNetCore.Auth.Extensions;

/// <summary>
/// Provides extension methods for resolving and forwarding <see cref="ActorContext"/> across HTTP requests and claims principals.
/// </summary>
public static class ActorContextExtensions
{
    private const string LegacyS2SSchemeName = "InternalServiceToken";
    private const int MaximumIdentifierLength = 36;
    private const int MaximumEmailLength = 254;
    private const int MaximumNameLength = 128;
    private const int MaximumRoleLength = 64;
    private const int MaximumServiceNameLength = 64;
    private const int MaximumTraceHeaderLength = 128;

    private static readonly string[] ActorHeaderNames =
    [
        CustomRequestHeaders.ActorType,
        CustomRequestHeaders.ActorId,
        CustomRequestHeaders.ActorRole,
        CustomRequestHeaders.UserId,
        CustomRequestHeaders.EmployeeId,
        CustomRequestHeaders.UserEmail,
        CustomRequestHeaders.UserName,
        CustomRequestHeaders.ServiceName
    ];

    /// <summary>
    /// Extracts an <see cref="ActorContext"/> from an authenticated claims principal.
    /// Distinguishes between End-User clients and internal Employees.
    /// </summary>
    public static ActorContext ResolveActorContext(this ClaimsPrincipal? principal)
    {
        if (principal is null)
        {
            return ActorContext.AnonymousContext;
        }

        var authenticatedIdentities = principal.Identities
            .Where(identity => identity.IsAuthenticated)
            .ToArray();
        if (authenticatedIdentities.Length == 0)
        {
            return ActorContext.AnonymousContext;
        }

        var hasTrustedServiceIdentity = authenticatedIdentities.Any(IsTrustedServiceIdentity);
        var hasAuthenticatedUserIdentity = authenticatedIdentities.Any(identity => !IsTrustedServiceIdentity(identity));
        if (hasTrustedServiceIdentity && hasAuthenticatedUserIdentity)
        {
            return ActorContext.AnonymousContext;
        }

        if (hasTrustedServiceIdentity)
        {
            return ActorContext.ForService("S2SGateway", "internal");
        }

        var identity = authenticatedIdentities[0];
        if (identity is null)
        {
            return ActorContext.AnonymousContext;
        }

        var identifier = identity.FindFirst(ClaimTypes.NameIdentifier)?.Value
                      ?? identity.FindFirst("sub")?.Value
                      ?? identity.FindFirst("user_id")?.Value
                      ?? identity.FindFirst("id")?.Value;

        if (!Guid.TryParse(identifier, out var actorId) || actorId == Guid.Empty)
        {
            return ActorContext.AnonymousContext;
        }

        var role = identity.FindFirst(ClaimTypes.Role)?.Value
                ?? identity.FindFirst("role")?.Value;

        var userTypeClaim = identity.FindFirst("user_type")?.Value
                         ?? identity.FindFirst("actor_type")?.Value;

        var isEmployee = string.Equals(userTypeClaim, "Employee", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(userTypeClaim, "Staff", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(userTypeClaim, "Backoffice", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(role, "Admin", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(role, "Operator", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(role, "Employee", StringComparison.OrdinalIgnoreCase) ||
                         identity.FindAll(ClaimTypes.Role).Any(c => 
                             string.Equals(c.Value, "Admin", StringComparison.OrdinalIgnoreCase) || 
                             string.Equals(c.Value, "Operator", StringComparison.OrdinalIgnoreCase) ||
                             string.Equals(c.Value, "Employee", StringComparison.OrdinalIgnoreCase));

        var emailClaim = identity.FindFirst(ClaimTypes.Email)?.Value
                      ?? identity.FindFirst("email")?.Value;
        var usernameClaim = identity.FindFirst(ClaimTypes.Name)?.Value
                         ?? identity.FindFirst("preferred_username")?.Value
                         ?? identity.FindFirst("username")?.Value;
        var email = !string.IsNullOrWhiteSpace(emailClaim)
            ? emailClaim
            : usernameClaim?.Contains('@') == true ? usernameClaim : null;
        var displayName = identity.FindFirst(ClaimTypes.Name)?.Value
                       ?? identity.FindFirst("name")?.Value
                       ?? usernameClaim;

        return isEmployee
            ? ActorContext.ForEmployee(actorId, email, displayName, role)
            : ActorContext.ForUser(actorId, email, displayName, role);
    }

    /// <summary>
    /// Extracts an <see cref="ActorContext"/> from an HTTP context. Delegated actor headers are
    /// trusted only after the request has been authenticated by a supported S2S scheme.
    /// </summary>
    public static ActorContext ResolveActorContextFromHttpContext(this HttpContext? httpContext)
    {
        if (httpContext is null)
        {
            return ActorContext.AnonymousContext;
        }

        var principalContext = httpContext.User.ResolveActorContext();
        if (principalContext is { IsAnonymous: false, IsService: false })
        {
            return principalContext;
        }

        if (!principalContext.IsService)
        {
            return ActorContext.AnonymousContext;
        }

        var headers = httpContext.Request.Headers;
        var hasDelegatedUser = headers.ContainsKey(CustomRequestHeaders.UserId);
        var hasDelegatedEmployee = headers.ContainsKey(CustomRequestHeaders.EmployeeId);

        if (hasDelegatedUser && hasDelegatedEmployee)
        {
            return ActorContext.AnonymousContext;
        }

        if (hasDelegatedUser)
        {
            if (!TryReadGuid(headers, CustomRequestHeaders.UserId, out var userId) ||
                !TryReadOptional(headers, CustomRequestHeaders.UserEmail, MaximumEmailLength, out var email) ||
                !TryReadOptional(headers, CustomRequestHeaders.UserName, MaximumNameLength, out var name) ||
                !TryReadOptional(headers, CustomRequestHeaders.ActorRole, MaximumRoleLength, out var role) ||
                !HasExpectedActorType(headers, ActorType.User) ||
                !HasExpectedActorId(headers, userId))
            {
                return ActorContext.AnonymousContext;
            }

            return ActorContext.ForUser(userId, email, name ?? email, role);
        }

        if (hasDelegatedEmployee)
        {
            if (!TryReadGuid(headers, CustomRequestHeaders.EmployeeId, out var employeeId) ||
                !TryReadOptional(headers, CustomRequestHeaders.UserEmail, MaximumEmailLength, out var email) ||
                !TryReadOptional(headers, CustomRequestHeaders.UserName, MaximumNameLength, out var name) ||
                !TryReadOptional(headers, CustomRequestHeaders.ActorRole, MaximumRoleLength, out var role) ||
                !HasExpectedActorType(headers, ActorType.Employee) ||
                !HasExpectedActorId(headers, employeeId))
            {
                return ActorContext.AnonymousContext;
            }

            return ActorContext.ForEmployee(employeeId, email, name ?? email, role);
        }

        if (!TryReadOptional(headers, CustomRequestHeaders.ServiceName, MaximumServiceNameLength, out var serviceName) ||
            !HasExpectedActorType(headers, ActorType.Service) ||
            headers.ContainsKey(CustomRequestHeaders.ActorId))
        {
            return ActorContext.AnonymousContext;
        }

        return ActorContext.ForService(serviceName ?? "InternalService", "internal");
    }

    /// <summary>
    /// Alias for <see cref="ResolveActorContext(ClaimsPrincipal?)"/>.
    /// </summary>
    public static ActorContext GetActorContext(this ClaimsPrincipal? principal) => principal.ResolveActorContext();

    /// <summary>
    /// Applies bounded actor-context headers to an outgoing request, replacing any pre-existing actor headers.
    /// </summary>
    public static void ApplyActorContext(this HttpRequestMessage request, ActorContext? actorContext)
    {
        ArgumentNullException.ThrowIfNull(request);
        ClearActorHeaders(request);

        if (actorContext is null || actorContext.IsAnonymous)
        {
            return;
        }

        SetHeader(request, CustomRequestHeaders.ActorType, actorContext.ActorType.ToString(), MaximumRoleLength);

        if (actorContext.ActorId is { } actorId && actorId != Guid.Empty)
        {
            var identifier = actorId.ToString("D");
            SetHeader(request, CustomRequestHeaders.ActorId, identifier, MaximumIdentifierLength);

            if (actorContext.IsUser)
            {
                SetHeader(request, CustomRequestHeaders.UserId, identifier, MaximumIdentifierLength);
            }
            else if (actorContext.IsEmployee)
            {
                SetHeader(request, CustomRequestHeaders.EmployeeId, identifier, MaximumIdentifierLength);
            }
        }

        if (actorContext.IsService)
        {
            SetHeader(request, CustomRequestHeaders.ServiceName, actorContext.DisplayName, MaximumServiceNameLength);
        }
        else
        {
            SetHeader(request, CustomRequestHeaders.UserEmail, actorContext.Email, MaximumEmailLength);
            SetHeader(request, CustomRequestHeaders.UserName, actorContext.DisplayName, MaximumNameLength);
        }

        SetHeader(request, CustomRequestHeaders.ActorRole, actorContext.Role, MaximumRoleLength);
    }

    /// <summary>
    /// Resolves actor and tracing context from an incoming request and applies bounded values to an outgoing request.
    /// </summary>
    public static void ApplyActorContext(this HttpRequestMessage request, HttpContext? httpContext)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (httpContext is null)
        {
            ClearActorHeaders(request);
            request.Headers.Remove(CustomRequestHeaders.CorrelationId);
            request.Headers.Remove(CustomRequestHeaders.RequestId);
            return;
        }

        request.ApplyActorContext(httpContext.ResolveActorContextFromHttpContext());

        request.Headers.Remove(CustomRequestHeaders.CorrelationId);
        request.Headers.Remove(CustomRequestHeaders.RequestId);

        if (TryReadOptional(httpContext.Request.Headers, CustomRequestHeaders.CorrelationId, MaximumTraceHeaderLength, out var correlationId))
        {
            SetHeader(request, CustomRequestHeaders.CorrelationId, correlationId, MaximumTraceHeaderLength);
        }

        if (TryReadOptional(httpContext.Request.Headers, CustomRequestHeaders.RequestId, MaximumTraceHeaderLength, out var requestId))
        {
            SetHeader(request, CustomRequestHeaders.RequestId, requestId, MaximumTraceHeaderLength);
        }
    }

    private static bool IsTrustedServiceIdentity(ClaimsIdentity identity)
        => identity.IsAuthenticated &&
           (string.Equals(identity.AuthenticationType, S2SAuthOptions.SchemeName, StringComparison.Ordinal) ||
            string.Equals(identity.AuthenticationType, LegacyS2SSchemeName, StringComparison.Ordinal)) &&
           identity.HasClaim("identity_type", "service");

    private static bool TryReadGuid(IHeaderDictionary headers, string headerName, out Guid identifier)
    {
        identifier = Guid.Empty;
        return TryReadRequired(headers, headerName, MaximumIdentifierLength, out var value) &&
               Guid.TryParse(value, out identifier) &&
               identifier != Guid.Empty;
    }

    private static bool HasExpectedActorType(IHeaderDictionary headers, ActorType expected)
    {
        if (!TryReadOptional(headers, CustomRequestHeaders.ActorType, MaximumRoleLength, out var actorType))
        {
            return false;
        }

        return actorType is null || string.Equals(actorType, expected.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    private static bool HasExpectedActorId(IHeaderDictionary headers, Guid expected)
    {
        if (!headers.ContainsKey(CustomRequestHeaders.ActorId))
        {
            return true;
        }

        return TryReadGuid(headers, CustomRequestHeaders.ActorId, out var actorId) && actorId == expected;
    }

    private static bool TryReadOptional(
        IHeaderDictionary headers,
        string headerName,
        int maximumLength,
        out string? value)
    {
        value = null;
        if (!headers.TryGetValue(headerName, out var values))
        {
            return true;
        }

        if (values.Count != 1)
        {
            return false;
        }

        var candidate = values[0];
        if (string.IsNullOrWhiteSpace(candidate))
        {
            return true;
        }

        if (!IsSafeHeaderValue(candidate, maximumLength))
        {
            return false;
        }

        value = candidate;
        return true;
    }

    private static bool TryReadRequired(
        IHeaderDictionary headers,
        string headerName,
        int maximumLength,
        out string value)
    {
        value = string.Empty;
        if (!TryReadOptional(headers, headerName, maximumLength, out var candidate) || candidate is null)
        {
            return false;
        }

        value = candidate;
        return true;
    }

    private static bool IsSafeHeaderValue(string value, int maximumLength)
        => value.Length <= maximumLength && !value.Any(char.IsControl);

    private static void ClearActorHeaders(HttpRequestMessage request)
    {
        foreach (var headerName in ActorHeaderNames)
        {
            request.Headers.Remove(headerName);
        }
    }

    private static void SetHeader(HttpRequestMessage request, string name, string? value, int maximumLength)
    {
        request.Headers.Remove(name);
        if (string.IsNullOrWhiteSpace(value) || !IsSafeHeaderValue(value, maximumLength))
        {
            return;
        }

        request.Headers.TryAddWithoutValidation(name, value);
    }
}
