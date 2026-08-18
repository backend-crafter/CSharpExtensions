using Microsoft.AspNetCore.Mvc;

namespace CSharpExtensions.AspNetCore.AspNet.Extensions;

/// <summary>
/// Extension methods for <see cref="ActionContext"/> and <see cref="ControllerBase"/>.
/// </summary>
public static class ActionContextExtensions
{
    /// <summary>
    /// Creates a <see cref="ValidationProblemDetails"/> from the current model state.
    /// </summary>
    public static ValidationProblemDetails CreateValidationProblemDetails(this ActionContext actionContext)
    {
        return ExceptionExtensions.CreateValidationProblemDetails(actionContext);
    }

    /// <summary>
    /// Creates a <see cref="BadRequestObjectResult"/> with standardized validation problem details.
    /// </summary>
    public static BadRequestObjectResult ToValidationProblemResult(this ActionContext actionContext)
    {
        return new BadRequestObjectResult(actionContext.CreateValidationProblemDetails());
    }
}
