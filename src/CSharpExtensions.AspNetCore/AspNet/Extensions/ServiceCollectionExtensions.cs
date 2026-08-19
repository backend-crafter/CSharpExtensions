using CSharpExtensions.AspNetCore.AspNet.Handlers;
using CSharpExtensions.AspNetCore.AspNet.Profiles;
using CSharpExtensions.AspNetCore.AspNet.Transformers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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
    /// Adds native ASP.NET Core OpenAPI services for modern API documentation tools such as Scalar.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="documentName">The OpenAPI document name (defaults to "v1").</param>
    /// <param name="configure">Optional delegate to configure OpenAPI options.</param>
    public static IServiceCollection AddOpenApiDocumentation(
        this IServiceCollection services,
        string documentName = "v1",
        Action<Microsoft.AspNetCore.OpenApi.OpenApiOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOpenApi(documentName, options =>
        {
            configure?.Invoke(options);
        });

        return services;
    }
}
