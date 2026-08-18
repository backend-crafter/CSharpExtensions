using CSharpExtensions.AspNetCore.AspNet.Contexts;
using CSharpExtensions.AspNetCore.AspNet.Profiles;
using CSharpExtensions.Core.Railway;
using Microsoft.AspNetCore.Mvc;

namespace CSharpExtensions.AspNetCore.AspNet.Transformers;

public class DefaultResultTransformer : IResultTransformer
{
    public ActionResult Transform(Result result, IActionResultProfile profile)
    {
        return result.IsFailure
            ? profile.TransformToFailureActionResult(new ActionResultProfileFailureContext(result.Error))
            : profile.TransformToSuccessActionResult(new ActionResultProfileSuccessContext(result));
    }

    public ActionResult<TValue> Transform<TValue>(Result<TValue> result, IActionResultProfile profile)
    {
        return result.IsFailure
            ? profile.TransformToFailureActionResult(new ActionResultProfileFailureContext(result.Error))
            : profile.TransformToSuccessActionResult<TValue>(new ActionResultProfileSuccessContext<TValue>(result));
    }
}
