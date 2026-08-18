using System.Diagnostics;
using CSharpExtensions.Foundation.Helpers;
using CSharpExtensions.Foundation.Helpers.Constants;
using Microsoft.AspNetCore.Http;

namespace CSharpExtensions.AspNetCore.AspNet.Middleware;

/// <summary>
/// Middleware for managing the Correlation ID across the request lifecycle.
/// Ensures x-correlation-id is present in both request and response.
/// </summary>
public sealed class CorrelationIdMiddleware(RequestDelegate next)
{
    public async Task InvokeAsync(HttpContext context)
    {
        var correlationId = GetCorrelationId(context);

        // Replace an untrusted or ambiguous inbound value at the application boundary.
        context.Request.Headers[CustomRequestHeaders.CorrelationId] = correlationId;
        context.Items[CustomRequestHeaders.CorrelationId] = correlationId;

        context.Response.OnStarting(() =>
        {
            context.Response.Headers[CustomRequestHeaders.CorrelationId] = correlationId;
            return Task.CompletedTask;
        });

        await next(context);
    }

    private static string GetCorrelationId(HttpContext context)
    {
        if (context.Request.Headers.TryGetValue(CustomRequestHeaders.CorrelationId, out var values) &&
            values.Count == 1 &&
            BoundedIdentifier.TryNormalize(values[0], out var supplied))
        {
            return supplied;
        }

        var generated = Activity.Current?.TraceId.ToString();
        if (BoundedIdentifier.TryNormalize(generated, out var traceIdentifier))
        {
            return traceIdentifier;
        }

        if (BoundedIdentifier.TryNormalize(context.TraceIdentifier, out var contextIdentifier))
        {
            return contextIdentifier;
        }

        return Guid.NewGuid().ToString("N");
    }
}
