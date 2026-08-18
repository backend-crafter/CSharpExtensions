using System.Globalization;
using System.Reflection;
using CSharpExtensions.AspNetCore.AspNet.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace CSharpExtensions.AspNetCore.AspNet.Extensions;

/// <summary>
/// Extension methods for standardized health checks.
/// </summary>
public static class HealthCheckExtensions
{
    /// <summary>
    /// Writes a unified JSON health response to the HttpContext.
    /// Returns 200 OK for Healthy, 503 Service Unavailable for Unhealthy.
    /// </summary>
    public static async Task WriteHealthResponse(HttpContext context, HealthReport report)
    {
        await WriteHealthResponse(context, report, context.RequestAborted).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes an authenticated detailed health response.
    /// </summary>
    public static async Task WriteHealthResponse(
        HttpContext context,
        HealthReport report,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(report);

        if (context.User?.Identities.Any(static identity => identity.IsAuthenticated) != true)
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return;
        }

        context.Response.ContentType = "application/json";
        
        context.Response.StatusCode = report.Status == HealthStatus.Healthy 
            ? StatusCodes.Status200OK 
            : StatusCodes.Status503ServiceUnavailable;

        // Try to find the entry point assembly to extract metadata
        var entryAssembly = Assembly.GetEntryAssembly();
        var metadata = entryAssembly?.GetCustomAttributes<AssemblyMetadataAttribute>().ToList();
        
        var commitHash = metadata?.FirstOrDefault(a => a.Key == "GitCommitHash")?.Value;
        var branch = metadata?.FirstOrDefault(a => a.Key == "GitBranch")?.Value;
        var gitVersion = metadata?.FirstOrDefault(a => a.Key == "GitVersion")?.Value;
        var buildTimestamp = metadata?.FirstOrDefault(a => a.Key == "BuildTimestamp")?.Value;
        
        var response = new HealthResponse(
            report.Status.ToString(),
            new HealthVersion(
                commitHash ?? "N/A",
                branch ?? "N/A",
                gitVersion ?? "N/A",
                buildTimestamp ?? "N/A"
            ),
            report.TotalDuration.TotalSeconds.ToString("0.00", CultureInfo.InvariantCulture) + "s",
            report.Entries.Select(e => new HealthResult(
                e.Key,
                e.Value.Status.ToString(),
                e.Value.Status == HealthStatus.Healthy ? "OK" : "Unhealthy"
            ))
        );

        await context.Response.WriteAsJsonAsync(response, cancellationToken);
    }

    /// <summary>
    /// Writes a minimal response suitable for anonymous orchestrator probes.
    /// </summary>
    public static Task WriteMinimalHealthResponse(
        HttpContext context,
        HealthReport report,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(report);

        context.Response.ContentType = "application/json";
        context.Response.StatusCode = report.Status == HealthStatus.Healthy
            ? StatusCodes.Status200OK
            : StatusCodes.Status503ServiceUnavailable;

        return context.Response.WriteAsJsonAsync(
            new { status = report.Status.ToString() },
            cancellationToken);
    }
}
