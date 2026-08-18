using CSharpExtensions.AspNetCore.AspNet.Configurations;
using CSharpExtensions.AspNetCore.AspNet.Transformers;
using CSharpExtensions.Core.Exceptions.Exceptions;
using CSharpExtensions.Core.Helpers;
using CSharpExtensions.Core.Railway;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Abstractions;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;

namespace CSharpExtensions.AspNetCore.AspNet.Handlers;

/// <summary>
/// Global exception handler for applications.
/// Maps ApiExceptions, standard .NET exceptions, and ROP Errors to RFC 9457 ProblemDetails via Railway transformation.
/// </summary>
public sealed class ApiExceptionHandler : IExceptionHandler
{
    private readonly ILogger<ApiExceptionHandler> _logger;
    private readonly IResultTransformer? _transformer;

    public ApiExceptionHandler(ILogger<ApiExceptionHandler> logger, IResultTransformer? transformer = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _transformer = transformer;
    }

    public async ValueTask<bool> TryHandleAsync(
        HttpContext httpContext,
        Exception exception,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(exception);

        if (httpContext.Response.HasStarted)
            return false;

        var traceIdentifier = BoundedIdentifier.TryNormalize(httpContext.TraceIdentifier, out var normalizedTraceIdentifier)
            ? normalizedTraceIdentifier
            : "invalid";
        _logger.LogError(
            exception,
            "Unhandled request failure. ExceptionType={ExceptionType} TraceIdentifier={TraceIdentifier}",
            exception.GetType().FullName,
            traceIdentifier);

        var transformer = _transformer ?? RailwayConfiguration.GetCurrentTransformer();
        var profile = RailwayConfiguration.GetCurrentProfile();

        var error = exception switch
        {
            ApiException apiException => new Error(apiException.Message).AsHttpStatus(apiException.HttpStatusCode, apiException.Type, apiException.Title),
            UnauthorizedAccessException => new Error(exception.Message).AsHttpStatus(StatusCodes.Status401Unauthorized, "Unauthorized", "Unauthorized"),
            _ => new Error("An unexpected error occurred.").AsHttpStatus(StatusCodes.Status500InternalServerError, "InternalServerError", "An internal server error occurred.")
        };

        var actionResult = transformer.Transform(Result.Failure(error), profile);
        var routeData = httpContext.GetRouteData() ?? new RouteData();
        var actionContext = new ActionContext(httpContext, routeData, new ActionDescriptor());

        await actionResult.ExecuteResultAsync(actionContext).ConfigureAwait(false);

        return true;
    }
}
