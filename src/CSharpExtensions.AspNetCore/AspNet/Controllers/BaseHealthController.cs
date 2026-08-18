using CSharpExtensions.AspNetCore.AspNet.Extensions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace CSharpExtensions.AspNetCore.AspNet.Controllers;

/// <summary>
/// Base controller for providing system health information.
/// </summary>
[ApiController]
[Route("api/v1/health")]
public abstract class BaseHealthController(
    HealthCheckService healthCheckService) : ControllerBase
{
    /// <summary>
    /// Performs a comprehensive health check of all dependencies.
    /// </summary>
    [Authorize]
    [HttpGet]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status503ServiceUnavailable)]
    public virtual async Task GetHealth()
    {
        if (HttpContext.User?.Identities.Any(static identity => identity.IsAuthenticated) != true)
        {
            HttpContext.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return;
        }

        var cancellationToken = HttpContext.RequestAborted;
        var report = await healthCheckService.CheckHealthAsync(cancellationToken);
        await HealthCheckExtensions.WriteHealthResponse(HttpContext, report, cancellationToken);
    }

    /// <summary>
    /// Liveness check - ensures the application is running.
    /// </summary>
    [AllowAnonymous]
    [HttpGet("live")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public virtual async Task GetLive()
    {
        // For liveness, we usually don't check dependencies
        var cancellationToken = HttpContext.RequestAborted;
        var report = await healthCheckService.CheckHealthAsync(_ => false, cancellationToken);
        await HealthCheckExtensions.WriteMinimalHealthResponse(HttpContext, report, cancellationToken);
    }

    /// <summary>
    /// Readiness check - ensures all dependencies are reachable and ready.
    /// </summary>
    [AllowAnonymous]
    [HttpGet("ready")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status503ServiceUnavailable)]
    public virtual async Task GetReady()
    {
        var cancellationToken = HttpContext.RequestAborted;
        var report = await healthCheckService.CheckHealthAsync(check => check.Tags.Contains("ready"), cancellationToken);
        await HealthCheckExtensions.WriteMinimalHealthResponse(HttpContext, report, cancellationToken);
    }

    /// <summary>
    /// Startup check - ensures initial connectivity for application startup.
    /// </summary>
    [AllowAnonymous]
    [HttpGet("startup")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status503ServiceUnavailable)]
    public virtual async Task GetStartup()
    {
        var cancellationToken = HttpContext.RequestAborted;
        var report = await healthCheckService.CheckHealthAsync(check => check.Tags.Contains("startup"), cancellationToken);
        await HealthCheckExtensions.WriteMinimalHealthResponse(HttpContext, report, cancellationToken);
    }
}
