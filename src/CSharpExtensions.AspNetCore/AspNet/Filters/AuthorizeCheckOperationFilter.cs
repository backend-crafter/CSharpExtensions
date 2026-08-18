using Microsoft.AspNetCore.Authorization;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace CSharpExtensions.AspNetCore.AspNet.Filters;

/// <summary>
/// An operation filter for Swagger that detects the [Authorize] attribute on controllers or actions.
/// It automatically adds 401 (Unauthorized) and 403 (Forbidden) response types to the documentation
/// and attaches the "Bearer" security requirement to restricted endpoints in the Swagger UI.
/// </summary>
public class AuthorizeCheckOperationFilter : IOperationFilter
{
    public void Apply(OpenApiOperation operation, OperationFilterContext context)
    {
        var hasAuthorize = context.MethodInfo.DeclaringType != null && 
                           (context.MethodInfo.DeclaringType.GetCustomAttributes(true).OfType<AuthorizeAttribute>().Any() || 
                            context.MethodInfo.GetCustomAttributes(true).OfType<AuthorizeAttribute>().Any());

        if (hasAuthorize)
        {
            operation.Responses.TryAdd("401", new OpenApiResponse { Description = "Unauthorized" });
            operation.Responses.TryAdd("403", new OpenApiResponse { Description = "Forbidden" });

            var oAuthScheme = new OpenApiSecurityScheme
            {
                Reference = new OpenApiReference { Type = ReferenceType.SecurityScheme, Id = "Bearer" }
            };

            operation.Security = new List<OpenApiSecurityRequirement>
            {
                new()
                {
                    [oAuthScheme] = new List<string>()
                }
            };
        }
    }
}