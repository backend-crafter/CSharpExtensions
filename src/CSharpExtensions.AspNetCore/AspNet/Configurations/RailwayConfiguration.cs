using CSharpExtensions.AspNetCore.AspNet.Profiles;
using CSharpExtensions.AspNetCore.AspNet.Transformers;

namespace CSharpExtensions.AspNetCore.AspNet.Configurations;

/// <summary>
/// Thread-safe immutable configuration for Railway ASP.NET Core integration.
/// </summary>
public static class RailwayConfiguration
{
    private static ActionResultProfileSettings _settings = new();
    private static int _isConfigured;

    internal static ActionResultProfileSettings ActionResultProfileSettings => Volatile.Read(ref _settings);

    /// <summary>
    /// Configures the Railway action result settings during application startup.
    /// </summary>
    public static void Setup(Action<ActionResultProfileSettings> setupFunc)
    {
        ArgumentNullException.ThrowIfNull(setupFunc);
        var settingsBuilder = new ActionResultProfileSettings();
        setupFunc(settingsBuilder);

        if (settingsBuilder.CurrentProfile is null)
        {
            throw new InvalidOperationException("A Railway action result profile must be configured.");
        }

        if (settingsBuilder.CurrentTransformer is null)
        {
            throw new InvalidOperationException("A Railway result transformer must be configured.");
        }

        Volatile.Write(ref _settings, settingsBuilder);
        Interlocked.Exchange(ref _isConfigured, 1);
    }

    /// <summary>
    /// Gets the current active action result profile.
    /// </summary>
    public static IActionResultProfile GetCurrentProfile()
    {
        return ActionResultProfileSettings.CurrentProfile
            ?? throw new InvalidOperationException("Railway is not configured. Call UseRailwayWithApiExceptions or UseApiExceptions during application startup.");
    }

    /// <summary>
    /// Gets the current active result transformer.
    /// </summary>
    public static IResultTransformer GetCurrentTransformer()
    {
        return ActionResultProfileSettings.CurrentTransformer
            ?? throw new InvalidOperationException("Railway result transformation is not configured.");
    }

    internal static (IActionResultProfile Profile, IResultTransformer Transformer) GetCurrent()
    {
        var settings = ActionResultProfileSettings;
        var profile = settings.CurrentProfile
            ?? throw new InvalidOperationException("Railway is not configured. Call UseRailwayWithApiExceptions or UseApiExceptions during application startup.");
        var transformer = settings.CurrentTransformer
            ?? throw new InvalidOperationException("Railway result transformation is not configured.");
        return (profile, transformer);
    }
}
