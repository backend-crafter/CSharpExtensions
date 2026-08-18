using CSharpExtensions.AspNetCore.AspNet.Profiles;
using CSharpExtensions.Foundation.Railway;
using Microsoft.AspNetCore.Mvc;

namespace CSharpExtensions.AspNetCore.AspNet.Transformers;

public interface IResultTransformer
{
    ActionResult Transform(Result result, IActionResultProfile profile);
    ActionResult<TValue> Transform<TValue>(Result<TValue> result, IActionResultProfile profile);
}
