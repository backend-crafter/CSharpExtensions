using CSharpExtensions.AspNetCore.AspNet.Configurations;
using CSharpExtensions.AspNetCore.AspNet.Middleware;
using CSharpExtensions.AspNetCore.AspNet.Profiles;
using CSharpExtensions.AspNetCore.AspNet.Transformers;
using CSharpExtensions.Foundation.Railway;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Scalar.AspNetCore;

namespace CSharpExtensions.AspNetCore.AspNet.Extensions;

/// <summary>
/// Extension methods for activating ASP.NET Core middleware and Railway ROP integration.
/// </summary>
public static class WebApplicationExtensions
{
    /// <summary>
    /// Activates global API exception handling. 
    /// This middleware should be registered as early as possible in the pipeline.
    /// </summary>
    public static IApplicationBuilder UseApiExceptions(this IApplicationBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);
        app.UseExceptionHandler();
        return app;
    }

    /// <summary>
    /// Activates global API exception handling and initializes Railway configurations and diagnostics.
    /// This middleware should be registered as early as possible in the pipeline.
    /// </summary>
    public static IApplicationBuilder UseRailwayWithApiExceptions(this IApplicationBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);
        var profile = app.ApplicationServices.GetRequiredService<IActionResultProfile>();
        var transformer = app.ApplicationServices.GetRequiredService<IResultTransformer>();
        var loggerFactory = app.ApplicationServices.GetRequiredService<ILoggerFactory>();

        RailwayConfiguration.Setup(settings =>
        {
            settings.CurrentProfile = profile;
            settings.CurrentTransformer = transformer;
        });
        RailwayDiagnostics.Configure(loggerFactory);
        
        app.UseExceptionHandler();

        return app;
    }

    /// <summary>
    /// Activates the Correlation ID middleware to trace requests.
    /// </summary>
    public static IApplicationBuilder UseCorrelationId(this IApplicationBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);
        return app.UseMiddleware<CorrelationIdMiddleware>();
    }
    
    /// <summary>
    /// Configures path base routing if PathBase is set in configuration.
    /// </summary>
    public static IApplicationBuilder AddPathBase(this IApplicationBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);

        var configuration = app.ApplicationServices.GetRequiredService<IConfiguration>();
        var pathBase = configuration["PathBase"];
        if (!string.IsNullOrEmpty(pathBase)) 
            app.UsePathBase(pathBase);

        if (app is IEndpointRouteBuilder endpointRouteBuilder)
        {
            endpointRouteBuilder.MapGet("/", () => "Service is running.")
                .AllowAnonymous()
                .ExcludeFromDescription();
        }

        return app;
    }

    /// <summary>
    /// Maps native OpenAPI endpoints and exposes the modern interactive Scalar API Reference UI.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="configureScalar">Optional delegate to customize Scalar UI options.</param>
    /// <returns>The endpoint route builder for method chaining.</returns>
    public static IEndpointRouteBuilder MapScalarDocumentation(
        this IEndpointRouteBuilder endpoints,
        Action<Scalar.AspNetCore.ScalarOptions>? configureScalar = null)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        endpoints.MapOpenApi();
        endpoints.MapScalarApiReference(options =>
        {
            options.WithTitle("CSharpExtensions API Reference")
                   .WithTheme(Scalar.AspNetCore.ScalarTheme.Moon);
            configureScalar?.Invoke(options);
        });

        return endpoints;
    }
}
