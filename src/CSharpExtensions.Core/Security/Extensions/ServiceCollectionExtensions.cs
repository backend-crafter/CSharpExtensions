using CSharpExtensions.Core.Security.Interfaces;
using CSharpExtensions.Core.Security.Options;
using CSharpExtensions.Core.Security.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.Core.Security.Extensions;

/// <summary>
/// Extension methods for registering security services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the high-precision encryption service for PII data.
    /// </summary>
    public static IServiceCollection AddEncryption(this IServiceCollection services, Action<EncryptionOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.AddEncryptionOptionsValidator();
        services.AddOptions<EncryptionOptions>()
            .Configure(configure)
            .ValidateOnStart();
        services.TryAddSingleton<IEncryptionService, EncryptionService>();
        return services;
    }

    /// <summary>
    /// Adds the high-precision encryption service using a configuration section.
    /// </summary>
    public static IServiceCollection AddEncryption(this IServiceCollection services, IConfigurationSection section)
    {
        ArgumentNullException.ThrowIfNull(section);
        services.AddEncryptionOptionsValidator();
        services.AddOptions<EncryptionOptions>()
            .Bind(section)
            .ValidateOnStart();
        services.TryAddSingleton<IEncryptionService, EncryptionService>();
        return services;
    }

    /// <summary>
    /// Adds the cryptographically secure OTP generator service.
    /// </summary>
    public static IServiceCollection AddOtpGenerator(this IServiceCollection services)
    {
        services.TryAddSingleton<IOtpGenerator, SecureOtpGenerator>();
        return services;
    }

    /// <summary>
    /// Adds identifier services to the specified IServiceCollection.
    /// </summary>
    /// <param name="services">The IServiceCollection to add services to.</param>
    /// <param name="configureOptions">An optional action to configure IdentifierOptions.</param>
    /// <returns>The IServiceCollection so that additional calls can be chained.</returns>
    public static IServiceCollection AddIdentifierService(
        this IServiceCollection services,
        Action<IdentifierOptions>? configureOptions = null)
    {
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<IdentifierOptions>, IdentifierOptionsValidator>());
        var optionsBuilder = services.AddOptions<IdentifierOptions>();
        if (configureOptions is not null)
        {
            optionsBuilder.Configure(configureOptions);
        }

        optionsBuilder.ValidateOnStart();
        services.TryAddSingleton<IIdentifierService, SqidsIdentifierService>();

        return services;
    }

    private static void AddEncryptionOptionsValidator(this IServiceCollection services)
    {
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<EncryptionOptions>, EncryptionOptionsValidator>());
    }
}
