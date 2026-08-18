using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.AspNetCore.AspNet.Contexts;

public class ActionResultProfileSuccessContext
{
    public Result Result { get; }

    public ActionResultProfileSuccessContext(Result result)
    {
        Result = result;
    }
}

public class ActionResultProfileSuccessContext<TValue>
{
    public Result<TValue> Result { get; }

    public ActionResultProfileSuccessContext(Result<TValue> result)
    {
        Result = result;
    }
}
