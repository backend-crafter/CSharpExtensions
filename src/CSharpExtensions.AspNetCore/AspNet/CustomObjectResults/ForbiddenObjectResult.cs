using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CSharpExtensions.AspNetCore.AspNet.CustomObjectResults;

/// <summary>
/// ActionResult that returns a 403 Forbidden error.
/// </summary>
public class ForbiddenObjectResult : ObjectResult
{
    public ForbiddenObjectResult(object? value) : base(value)
    {
        StatusCode = StatusCodes.Status403Forbidden;
    }
}
