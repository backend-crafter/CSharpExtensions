using System.Reflection;
using CSharpExtensions.AspNetCore.AspNet.Configurations;
using CSharpExtensions.AspNetCore.AspNet.Middleware;
using CSharpExtensions.AspNetCore.AspNet.Profiles;
using CSharpExtensions.AspNetCore.AspNet.Transformers;
using CSharpExtensions.Foundation.Railway;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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
    /// Enables Swagger and Swagger UI middleware with dynamic endpoint discovery.
    /// </summary>
    /// <param name="app">The <see cref="IApplicationBuilder"/> to configure.</param>
    /// <param name="targetAssembly">The assembly where controllers are located (e.g., typeof(Program).Assembly).</param>
    /// <returns>The same application builder so that multiple calls can be chained.</returns>
    public static IApplicationBuilder UseSwaggerDocumentation(this IApplicationBuilder app, Assembly targetAssembly)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(targetAssembly);

        app.UseSwagger();
        app.UseSwaggerUI(options => 
        {
            var groups = targetAssembly.GetTypes()
                .Where(t => typeof(ControllerBase).IsAssignableFrom(t))
                .Select(t => t.GetCustomAttribute<ApiExplorerSettingsAttribute>()?.GroupName)
                .Where(g => !string.IsNullOrEmpty(g))
                .Distinct()
                .OrderBy(g => g);

            foreach (var group in groups)
            {
                var encodedGroup = Uri.EscapeDataString(group!);
                options.SwaggerEndpoint($"{encodedGroup}/swagger.json", group);
            }
        });
        
        return app;
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
}
