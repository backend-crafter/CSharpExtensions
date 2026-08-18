using System.Diagnostics;
using CSharpExtensions.AspNetCore.AspNet.Configurations;
using CSharpExtensions.AspNetCore.AspNet.Contexts;
using CSharpExtensions.Core.Exceptions.Exceptions;
using CSharpExtensions.Core.Helpers.Constants;
using CSharpExtensions.Core.Railway;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;

namespace CSharpExtensions.AspNetCore.AspNet.Extensions;

/// <summary>
/// Extension methods for mapping exceptions and ROP Errors to RFC 9457 ProblemDetails.
/// </summary>
public static class ExceptionExtensions
{
    public static ProblemDetails CreateProblemDetails(
        HttpContext httpContext,
        int statusCode,
        string type,
        string title,
        string detail)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        var problemDetails = new ProblemDetails
        {
            Status = statusCode,
            Type = type,
            Title = title,
            Detail = detail,
            Instance = httpContext.Request.Path
        };

        var traceId = Activity.Current?.Id ?? httpContext.TraceIdentifier;
        if (!string.IsNullOrEmpty(traceId))
        {
            problemDetails.Extensions["traceId"] = traceId;
        }

        if (httpContext.Request.Headers.TryGetValue(CustomRequestHeaders.CorrelationId, out var correlationId))
        {
            problemDetails.Extensions["correlationId"] = correlationId.ToString();
        }

        return problemDetails;
    }

    public static ProblemDetails CreateProblemDetails(HttpContext httpContext, Exception exception)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(exception);

        if (exception is ApiException apiException)
        {
            return CreateProblemDetails(
                httpContext,
                apiException.HttpStatusCode,
                apiException.Type,
                apiException.Title,
                apiException.Message);
        }

        return CreateProblemDetails(
            httpContext,
            StatusCodes.Status500InternalServerError,
            "InternalServerError",
            "An unexpected error occurred.",
            "An internal server error occurred.");
    }

    public static ProblemDetails CreateProblemDetails(HttpContext httpContext, Error error)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(error);

        var problemDetails = CreateProblemDetails(
            httpContext,
            error.GetHttpStatusCode(),
            error.Type,
            error.Title,
            error.Message);

        problemDetails.Extensions["timestamp"] = error.Timestamp;

        if (error.Metadata is { Count: > 0 })
        {
            foreach (var (key, value) in error.Metadata)
            {
                problemDetails.Extensions[key] = value;
            }
        }

        return problemDetails;
    }

    public static ProblemDetails ToProblemDetails(this Error error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return RailwayConfiguration.GetCurrentProfile().TransformToProblemDetails(new ActionResultProfileFailureContext(error));
    }

    public static ProblemDetails ToProblemDetails(this Error error, HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(error);
        return CreateProblemDetails(httpContext, error);
    }

    public static ValidationProblemDetails CreateValidationProblemDetails(ActionContext actionContext)
    {
        ArgumentNullException.ThrowIfNull(actionContext);
        return CreateValidationProblemDetails(actionContext.HttpContext, actionContext.ModelState);
    }

    public static ValidationProblemDetails CreateValidationProblemDetails(
        HttpContext httpContext,
        ModelStateDictionary modelState)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(modelState);

        var errors = modelState
            .Where(e => e.Value?.Errors.Count > 0)
            .ToDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value!.Errors.Select(e => e.ErrorMessage).ToArray()
            );

        var validationProblemDetails = new ValidationProblemDetails(errors)
        {
            Status = StatusCodes.Status400BadRequest,
            Type = "ValidationError",
            Title = "One or more validation errors occurred.",
            Detail = "The request contains invalid parameters.",
            Instance = httpContext.Request.Path
        };

        var traceId = Activity.Current?.Id ?? httpContext.TraceIdentifier;
        if (!string.IsNullOrEmpty(traceId))
        {
            validationProblemDetails.Extensions["traceId"] = traceId;
        }

        if (httpContext.Request.Headers.TryGetValue(CustomRequestHeaders.CorrelationId, out var correlationId))
        {
            validationProblemDetails.Extensions["correlationId"] = correlationId.ToString();
        }

        return validationProblemDetails;
    }

    public static Error ToError(this ProblemDetails problemDetails)
    {
        ArgumentNullException.ThrowIfNull(problemDetails);

        var error = new Error(problemDetails.Detail ?? problemDetails.Title ?? "Unknown error");

        var type = problemDetails.Type ?? "Error";
        var title = problemDetails.Title ?? "Error";

        var statusCode = problemDetails.Status is >= 400 and <= 599
            ? problemDetails.Status.Value
            : StatusCodes.Status500InternalServerError;

        error.AsHttpStatus(statusCode, type, title);

        if (problemDetails.Extensions.TryGetValue("timestamp", out var ts) && ts is DateTime timestamp)
        {
            error.WithTimestamp(timestamp);
        }

        return error;
    }
}
