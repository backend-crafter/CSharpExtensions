using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.AspNetCore.AspNet.Contexts;

public class ActionResultProfileFailureContext
{
    public Error Error { get; }

    public ActionResultProfileFailureContext(Error error)
    {
        Error = error;
    }
}
