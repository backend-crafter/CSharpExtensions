using CSharpExtensions.Foundation.Helpers.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.Foundation.Helpers.Extensions;

public static class DatabaseOptionsServiceCollectionExtensions
{
    /// <summary>
    /// Registers and validates the shared database topology during startup.
    /// </summary>
    public static IServiceCollection AddDatabaseOptions(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddOptions<DatabasesOptions>()
            .Bind(configuration.GetSection(DatabasesOptions.SectionName))
            .ValidateOnStart();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<DatabasesOptions>, DatabasesOptionsValidator>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<ShardingOptions>, ShardingOptionsValidator>());
        return services;
    }
}
