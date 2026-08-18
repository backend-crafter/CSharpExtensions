using System;
using System.Collections.Generic;
using System.Linq;
using CSharpExtensions.Core.Helpers;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Any;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;
using CSharpExtensions.Kafka.Abstractions;

namespace CSharpExtensions.Kafka.Core;

/// <summary>
/// Swagger parameter filter to populate the 'topicConfigurationKey' parameter
/// with a dropdown list of all dynamically registered Kafka topics from options.
/// </summary>
public sealed class KafkaMaintenanceParameterFilter(IOptions<KafkaOptions> options) : IParameterFilter
{
    private const int MaximumExposedTopicKeys = 100;
    private const int MaximumExposedTopicKeyLength = 128;
    private readonly KafkaOptions _options = options?.Value ?? new KafkaOptions();

    /// <inheritdoc />
    public void Apply(OpenApiParameter parameter, ParameterFilterContext context)
    {
        if (parameter == null)
        {
            return;
        }

        if (string.Equals(parameter.Name, "topicConfigurationKey", StringComparison.OrdinalIgnoreCase))
        {
            parameter.Schema ??= new OpenApiSchema { Type = "string" };
            parameter.Schema.Enum ??= new List<IOpenApiAny>();
            parameter.Schema.Enum.Clear();
            parameter.Description = "Kafka topic configuration key.";

            if (_options.Maintenance?.ExposeTopicConfigurationKeysInOpenApi != true
                || _options.Topics is null
                || _options.Topics.Count is 0 or > MaximumExposedTopicKeys)
            {
                return;
            }

            var availableKeys = new List<string>(_options.Topics.Count);
            foreach (var key in _options.Topics.Keys)
            {
                if (!BoundedIdentifier.TryNormalize(key, out var normalizedKey, MaximumExposedTopicKeyLength))
                {
                    return;
                }

                availableKeys.Add(normalizedKey);
            }

            parameter.Schema.Enum ??= new List<IOpenApiAny>();
            foreach (var key in availableKeys.OrderBy(key => key, StringComparer.Ordinal))
            {
                parameter.Schema.Enum.Add(new OpenApiString(key));
            }

            parameter.Description = "Configured Kafka topic keys exposed by explicit maintenance OpenAPI opt-in.";
        }
    }
}
