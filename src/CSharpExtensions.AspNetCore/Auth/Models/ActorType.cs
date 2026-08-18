namespace CSharpExtensions.AspNetCore.Auth.Models;

/// <summary>
/// Specifies the type of actor executing a request or operation within the ecosystem.
/// </summary>
public enum ActorType
{
    /// <summary>
    /// Anonymous or unauthenticated actor.
    /// </summary>
    Anonymous = 0,

    /// <summary>
    /// End-user or client interacting with customer-facing services.
    /// </summary>
    User = 1,

    /// <summary>
    /// Internal employee, backoffice operator, or administrator.
    /// </summary>
    Employee = 2,

    /// <summary>
    /// Internal automated background process, worker, or service-to-service call.
    /// </summary>
    Service = 3
}
