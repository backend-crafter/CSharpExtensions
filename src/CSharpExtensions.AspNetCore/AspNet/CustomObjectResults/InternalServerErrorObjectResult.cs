using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CSharpExtensions.AspNetCore.AspNet.CustomObjectResults;

/// <summary>
/// ActionResult that returns a 500 Internal Server Error.
/// </summary>
public class InternalServerErrorObjectResult : ObjectResult
{
    public InternalServerErrorObjectResult(object? value) : base(value)
    {
        StatusCode = StatusCodes.Status500InternalServerError;
    }
}
