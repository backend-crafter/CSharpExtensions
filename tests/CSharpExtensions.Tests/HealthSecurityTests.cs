using System.Security.Claims;
using System.Text;
using CSharpExtensions.AspNetCore.AspNet.Controllers;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace CSharpExtensions.Tests;

public sealed class HealthSecurityTests
{
    [Fact]
    public async Task DetailedHealth_ShouldFailClosedWhenDerivedControllerAllowsAnonymous()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks();
        await using var provider = services.BuildServiceProvider();
        var controller = new AnonymousDerivedHealthController(
            provider.GetRequiredService<HealthCheckService>());
        var httpContext = new DefaultHttpContext();
        httpContext.Response.Body = new MemoryStream();
        controller.ControllerContext = new ControllerContext { HttpContext = httpContext };

        await controller.GetHealth();

        Assert.Equal(StatusCodes.Status401Unauthorized, httpContext.Response.StatusCode);
        Assert.Equal(0, httpContext.Response.Body.Length);
    }

    [Fact]
    public async Task DetailedHealth_ShouldRunForAuthenticatedIdentity()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks();
        await using var provider = services.BuildServiceProvider();
        var controller = new AnonymousDerivedHealthController(
            provider.GetRequiredService<HealthCheckService>());
        var httpContext = new DefaultHttpContext
        {
            User = new ClaimsPrincipal(new ClaimsIdentity(
                [new Claim(ClaimTypes.NameIdentifier, Guid.NewGuid().ToString("D"))],
                "test"))
        };
        httpContext.Response.Body = new MemoryStream();
        controller.ControllerContext = new ControllerContext { HttpContext = httpContext };

        await controller.GetHealth();

        Assert.Equal(StatusCodes.Status200OK, httpContext.Response.StatusCode);
        Assert.True(httpContext.Response.Body.Length > 0);
    }

    [Fact]
    public async Task DetailedHealth_ShouldNotExposeHealthCheckDescription()
    {
        const string sensitiveDescription = "server=db.internal;password=secret";
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks()
            .AddCheck("database", () => HealthCheckResult.Unhealthy(sensitiveDescription));
        await using var provider = services.BuildServiceProvider();
        var controller = new AnonymousDerivedHealthController(
            provider.GetRequiredService<HealthCheckService>());
        var httpContext = new DefaultHttpContext
        {
            User = new ClaimsPrincipal(new ClaimsIdentity(
                [new Claim(ClaimTypes.NameIdentifier, Guid.NewGuid().ToString("D"))],
                authenticationType: "Test"))
        };
        httpContext.Response.Body = new MemoryStream();
        controller.ControllerContext = new ControllerContext { HttpContext = httpContext };

        await controller.GetHealth();

        httpContext.Response.Body.Position = 0;
        using var reader = new StreamReader(httpContext.Response.Body, Encoding.UTF8);
        var body = await reader.ReadToEndAsync();
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, httpContext.Response.StatusCode);
        Assert.DoesNotContain(sensitiveDescription, body, StringComparison.Ordinal);
        Assert.Contains("Unhealthy", body, StringComparison.Ordinal);
    }

    [AllowAnonymous]
    private sealed class AnonymousDerivedHealthController(HealthCheckService healthCheckService)
        : BaseHealthController(healthCheckService);
}
