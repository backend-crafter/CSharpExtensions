using System.Reflection;
using CSharpExtensions.AspNetCore.AspNet.Filters;
using CSharpExtensions.AspNetCore.AspNet.Handlers;
using CSharpExtensions.AspNetCore.AspNet.Profiles;
using CSharpExtensions.AspNetCore.AspNet.Transformers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.OpenApi.Models;

namespace CSharpExtensions.AspNetCore.AspNet.Extensions;

/// <summary>
/// Extension methods for configuring ASP.NET Core tools and Railway ROP integration.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds Railway Oriented Programming (ROP) support, enabling Result to ActionResult transformations.
    /// </summary>
    public static IServiceCollection AddRailway(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddHttpContextAccessor();
        services.TryAddSingleton<IResultTransformer, DefaultResultTransformer>();
        services.TryAddSingleton<IActionResultProfile, ActionResultProfile>();
        
        return services;
    }

    /// <summary>
    /// Adds global exception handling support for ApiExceptions and standard .NET exceptions.
    /// Requires calling UseApiExceptions() in the middleware pipeline.
    /// </summary>
    public static IServiceCollection AddApiExceptionHandler(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddProblemDetails();
        services.AddExceptionHandler<ApiExceptionHandler>();
        
        return services;
    }

    /// <summary>
    /// Adds Railway ROP support and global API exception handling.
    /// </summary>
    public static IServiceCollection AddRailwayWithApiExceptions(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddRailway();
        services.AddApiExceptionHandler();
        
        return services;
    }
    
    /// <summary>
    /// Registers and configures Swagger generation with OpenAPI metadata and dynamic grouping.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add services to.</param>
    /// <param name="targetAssembly">The assembly where controllers are located (e.g., typeof(Program).Assembly).</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    public static IServiceCollection AddSwaggerDocumentation(this IServiceCollection services, Assembly targetAssembly)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(targetAssembly);

        services.AddEndpointsApiExplorer();
        services.AddSwaggerGen(c =>
        {
            var groups = targetAssembly.GetTypes()
                .Where(t => typeof(ControllerBase).IsAssignableFrom(t))
                .Select(t => t.GetCustomAttribute<ApiExplorerSettingsAttribute>()?.GroupName)
                .Where(g => !string.IsNullOrEmpty(g))
                .Distinct()
                .OrderBy(g => g);

            var metadata = targetAssembly.GetCustomAttributes<AssemblyMetadataAttribute>().ToList();
            var commitHash = metadata.FirstOrDefault(a => a.Key == "GitCommitHash")?.Value;
            var gitTag = metadata.FirstOrDefault(a => a.Key == "GitTag")?.Value;
            var gitBranch = metadata.FirstOrDefault(a => a.Key == "GitBranch")?.Value;
            var buildTimestamp = metadata.FirstOrDefault(a => a.Key == "BuildTimestamp")?.Value;
            
            var branchInfo = !string.IsNullOrEmpty(gitBranch) && gitBranch != "HEAD" && gitBranch != "N/A" 
                ? $"**Branch:** `{gitBranch}`" 
                : string.Empty;

            var tagInfo = !string.IsNullOrEmpty(gitTag) 
                ? $"**Tag:** `{gitTag}`" 
                : string.Empty;

            var commitInfo = !string.IsNullOrEmpty(commitHash) ? $"**Commit:** `{commitHash}`" : string.Empty;
            var buildInfo = !string.IsNullOrEmpty(buildTimestamp) ? $"**Built:** {buildTimestamp}" : string.Empty;

            var fullDescription = string.Join(" | ", new[] { branchInfo, tagInfo, commitInfo, buildInfo }.Where(s => !string.IsNullOrEmpty(s)));

            foreach (var group in groups)
            {
                c.SwaggerDoc(group, new OpenApiInfo
                {
                    Title = group,
                    Version = group!.Split(' ').Last(),
                    Description = fullDescription
                });
            }

            // Auth in Swagger
            c.AddSecurityDefinition("Bearer", new OpenApiSecurityScheme
            {
                Description = "JWT bearer access token.",
                Type = SecuritySchemeType.Http,
                Scheme = "bearer",
                BearerFormat = "JWT"
            });

            c.OperationFilter<AuthorizeCheckOperationFilter>();

            var xmlFile = $"{targetAssembly.GetName().Name}.xml";
            var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
            if (File.Exists(xmlPath))
            {
                c.IncludeXmlComments(xmlPath);
            }
        });

        return services;
    }
}
