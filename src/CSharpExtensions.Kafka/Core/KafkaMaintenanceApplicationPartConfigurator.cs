namespace CSharpExtensions.Kafka.Core;

using System.Linq;
using Microsoft.AspNetCore.Mvc.ApplicationParts;
using Microsoft.Extensions.Options;

/// <summary>
/// Configures the ASP.NET Core <see cref="ApplicationPartManager"/> to include
/// the Kafka maintenance controller assembly without calling <c>AddControllers()</c>,
/// which would override or interfere with the host application's MVC pipeline settings.
/// </summary>
internal sealed class KafkaMaintenanceApplicationPartConfigurator
    : IConfigureOptions<ApplicationPartManager>
{
    /// <inheritdoc />
    public void Configure(ApplicationPartManager partManager)
    {
        var controllerAssembly = typeof(KafkaMaintenanceController).Assembly;
        var assemblyPart = new AssemblyPart(controllerAssembly);

        var alreadyRegistered = partManager.ApplicationParts
            .Any(existingPart => existingPart.Name == assemblyPart.Name);

        if (!alreadyRegistered)
        {
            partManager.ApplicationParts.Add(assemblyPart);
        }
    }
}
