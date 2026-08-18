using CSharpExtensions.AspNetCore.Auth.Models;
using CSharpExtensions.Foundation.Exceptions.Exceptions;
using Microsoft.AspNetCore.Http;

namespace CSharpExtensions.AspNetCore.Auth.Extensions;

/// <summary>
/// Provides extension methods for <see cref="HttpContext"/> to resolve user and actor context.
/// </summary>
public static class HttpContextExtensions
{
    /// <summary>
    /// Resolves the <see cref="ActorContext"/> from the current HTTP context.
    /// </summary>
    /// <param name="httpContext">The HTTP context.</param>
    /// <returns>The resolved <see cref="ActorContext"/>.</returns>
    public static ActorContext ResolveActorContext(this HttpContext? httpContext)
    {
        return httpContext.ResolveActorContextFromHttpContext();
    }

    /// <summary>
    /// Requires an authenticated user context from the current HTTP context.
    /// Throws <see cref="UnauthorizedException"/> if user is not authenticated or claims are missing.
    /// </summary>
    /// <param name="httpContext">The HTTP context.</param>
    /// <returns>The resolved <see cref="UserContext"/>.</returns>
    public static UserContext RequireUserContext(this HttpContext? httpContext)
    {
        if (httpContext is null)
        {
            throw new UnauthorizedException("User context is required but user is not authenticated.");
        }

        var actor = httpContext.ResolveActorContextFromHttpContext();
        if (actor.IsAnonymous || actor.IsService || actor.ActorId is not { } actorId || actorId == Guid.Empty)
        {
            throw new UnauthorizedException("User context is required but user is not authenticated.");
        }

        var username = actor.DisplayName ?? actor.Email ?? actorId.ToString("D");

        if (string.IsNullOrWhiteSpace(username))
        {
            throw new UnauthorizedException("User context is required but user identity claim is missing.");
        }

        return new UserContext(username, actorId, actor.Email, actor.Role);
    }
}
