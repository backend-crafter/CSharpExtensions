using CSharpExtensions.AspNetCore.AspNet.Configurations;
using CSharpExtensions.AspNetCore.AspNet.Profiles;
using CSharpExtensions.Foundation.Railway;
using Microsoft.AspNetCore.Mvc;

namespace CSharpExtensions.AspNetCore.AspNet.Extensions;

/// <summary>
/// Provides extension methods for converting Railway Results to ASP.NET Core ActionResults.
/// </summary>
public static class ActionResultExtensions
{
    public static ActionResult ToActionResult(this Result result)
    {
        var current = RailwayConfiguration.GetCurrent();
        return current.Transformer.Transform(result, current.Profile);
    }

    public static async Task<ActionResult> ToActionResult(this Task<Result> resultTask)
    {
        var result = await resultTask.ConfigureAwait(false);
        return result.ToActionResult();
    }

    public static ActionResult ToActionResult(this Result result, IActionResultProfile profile)
    {
        return RailwayConfiguration.GetCurrentTransformer().Transform(result, profile);
    }

    public static async Task<ActionResult> ToActionResult(this Task<Result> resultTask, IActionResultProfile profile)
    {
        var result = await resultTask.ConfigureAwait(false);
        return result.ToActionResult(profile);
    }

    public static ActionResult<TValue> ToActionResult<TValue>(this Result<TValue> result)
    {
        var current = RailwayConfiguration.GetCurrent();
        return current.Transformer.Transform(result, current.Profile);
    }

    public static async Task<ActionResult<TValue>> ToActionResult<TValue>(this Task<Result<TValue>> resultTask)
    {
        var result = await resultTask.ConfigureAwait(false);
        return result.ToActionResult();
    }

    public static ActionResult<TValue> ToActionResult<TValue>(this Result<TValue> result, IActionResultProfile profile)
    {
        return RailwayConfiguration.GetCurrentTransformer().Transform(result, profile);
    }

    public static async Task<ActionResult<TValue>> ToActionResult<TValue>(this Task<Result<TValue>> resultTask, IActionResultProfile profile)
    {
        var result = await resultTask.ConfigureAwait(false);
        return result.ToActionResult(profile);
    }

    public static ActionResult ToActionResult(this Error error)
    {
        ArgumentNullException.ThrowIfNull(error);
        var current = RailwayConfiguration.GetCurrent();
        return current.Transformer.Transform(Result.Failure(error), current.Profile);
    }

    public static ActionResult ToActionResult(this Error error, IActionResultProfile profile)
    {
        ArgumentNullException.ThrowIfNull(error);
        ArgumentNullException.ThrowIfNull(profile);
        return RailwayConfiguration.GetCurrentTransformer().Transform(Result.Failure(error), profile);
    }
}
