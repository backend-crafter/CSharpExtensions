using CSharpExtensions.AspNetCore.AspNet.Transformers;

namespace CSharpExtensions.AspNetCore.AspNet.Profiles;

public class ActionResultProfileSettings
{
    public IActionResultProfile CurrentProfile { get; set; } = null!;

    public IResultTransformer CurrentTransformer { get; set; } = new DefaultResultTransformer();
}
