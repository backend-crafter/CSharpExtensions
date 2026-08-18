using CSharpExtensions.AspNetCore.AspNet.Contexts;
using Microsoft.AspNetCore.Mvc;

namespace CSharpExtensions.AspNetCore.AspNet.Profiles;

public interface IActionResultProfile
{
    ActionResult TransformToSuccessActionResult(ActionResultProfileSuccessContext context);
    ActionResult<TValue> TransformToSuccessActionResult<TValue>(ActionResultProfileSuccessContext<TValue> context);
    ActionResult TransformToFailureActionResult(ActionResultProfileFailureContext context);
    ProblemDetails TransformToProblemDetails(ActionResultProfileFailureContext context);
}
