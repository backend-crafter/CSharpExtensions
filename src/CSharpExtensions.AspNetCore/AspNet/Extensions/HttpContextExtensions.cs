using CSharpExtensions.Foundation.Helpers.Constants;
using Microsoft.AspNetCore.Http;

namespace CSharpExtensions.AspNetCore.AspNet.Extensions;

/// <summary>
/// Extension methods for <see cref="HttpContext"/> related to web operations.
/// </summary>
public static class HttpContextExtensions
{
    /// <summary>
    /// Extracts the API version from the request.
    /// Priority: 
    /// 1. Custom header 'x-api-version'
    /// 2. Query string parameter 'v'
    /// 3. Content-Type 'v=' attribute (Media Type Versioning)
    /// </summary>
    public static string GetApiVersion(this HttpContext httpContext)
    {
        // 1. Header
        if (httpContext.Request.Headers.TryGetValue(CustomRequestHeaders.ApiVersion, out var headerVersion))
        {
            return headerVersion.ToString();
        }

        // 2. Query String
        if (httpContext.Request.Query.TryGetValue("v", out var queryVersion))
        {
            return queryVersion.ToString();
        }

        // 3. Content-Type
        return GetVersionFromContentType(httpContext.Request.ContentType);
    }

    private static string GetVersionFromContentType(string? contentType)
    {
        if (string.IsNullOrWhiteSpace(contentType)) return string.Empty;

        var span = contentType.AsSpan();
        var vIndex = span.IndexOf("v=".AsSpan(), StringComparison.Ordinal);
        if (vIndex == -1) return string.Empty;

        var versionPart = span[(vIndex + 2)..];
        var endIndex = versionPart.IndexOfAny(' ', ';');
        
        return endIndex == -1 ? versionPart.ToString() : versionPart[..endIndex].ToString();
    }
}
