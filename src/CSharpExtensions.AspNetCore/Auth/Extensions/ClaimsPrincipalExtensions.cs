using System.Security.Claims;

namespace CSharpExtensions.AspNetCore.Auth.Extensions;

/// <summary>
/// Provides extension methods for <see cref="ClaimsPrincipal"/> to extract token details.
/// </summary>
public static class ClaimsPrincipalExtensions
{
    /// <summary>
    /// Attempts to extract the user/actor identifier from the claims principal.
    /// Checks NameIdentifier and sub claims.
    /// </summary>
    /// <param name="principal">The user principal.</param>
    /// <returns>The user/actor ID if found and valid; otherwise null.</returns>
    public static Guid? GetUserId(this ClaimsPrincipal principal)
    {
        if (principal is null) return null;

        var identity = principal.Identities.FirstOrDefault(candidate => candidate.IsAuthenticated);
        if (identity is null) return null;

        var subClaim = identity.FindFirst(ClaimTypes.NameIdentifier)?.Value
            ?? identity.FindFirst("sub")?.Value
            ?? identity.FindFirst("user_id")?.Value
            ?? identity.FindFirst("id")?.Value;

        return Guid.TryParse(subClaim, out var userId) && userId != Guid.Empty
            ? userId 
            : null;
    }

    /// <summary>
    /// Attempts to extract the actor ID from the claims principal.
    /// Alias for <see cref="GetUserId(ClaimsPrincipal)"/>.
    /// </summary>
    public static Guid? GetActorId(this ClaimsPrincipal principal) => principal.GetUserId();
}
