using CSharpExtensions.AspNetCore.AspNet.Contexts;
using CSharpExtensions.AspNetCore.AspNet.CustomObjectResults;
using CSharpExtensions.AspNetCore.AspNet.Extensions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CSharpExtensions.AspNetCore.AspNet.Profiles;

public record ActionResultProfile : IActionResultProfile
{
    private readonly IHttpContextAccessor _httpContextAccessor;

    public ActionResultProfile(IHttpContextAccessor httpContextAccessor)
    {
        _httpContextAccessor = httpContextAccessor;
    }

    public virtual ActionResult TransformToSuccessActionResult(ActionResultProfileSuccessContext context)
    {
        return new OkResult();
    }

    public virtual ActionResult<TValue> TransformToSuccessActionResult<TValue>(ActionResultProfileSuccessContext<TValue> context)
    {
        return new OkObjectResult(context.Result.ValueOrDefault);
    }

    public virtual ActionResult TransformToFailureActionResult(ActionResultProfileFailureContext context)
    {
        var problemDetails = TransformToProblemDetails(context);

        return problemDetails.Status switch
        {
            400 => new BadRequestObjectResult(problemDetails),
            401 => new UnauthorizedObjectResult(problemDetails),
            403 => new ForbiddenObjectResult(problemDetails),
            404 => new NotFoundObjectResult(problemDetails),
            >= 400 and <= 599 => new ObjectResult(problemDetails) { StatusCode = problemDetails.Status },
            _   => new InternalServerErrorObjectResult(problemDetails),
        };
    }

    public virtual ProblemDetails TransformToProblemDetails(ActionResultProfileFailureContext context)
    {
        var httpContext = _httpContextAccessor.HttpContext;

        if (httpContext == null)
        {
             throw new InvalidOperationException("HttpContext is not available. Ensure IHttpContextAccessor is registered.");
        }

        return ExceptionExtensions.CreateProblemDetails(httpContext, context.Error);
    }
}
