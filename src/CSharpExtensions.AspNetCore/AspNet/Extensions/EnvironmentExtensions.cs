using Microsoft.AspNetCore.Hosting;

namespace CSharpExtensions.AspNetCore.AspNet.Extensions;

public static class EnvironmentExtensions
{
    public static bool IsLocal(this IWebHostEnvironment env)
    {
        return env.EnvironmentName.ToLower() == "local";
    }
}