namespace CSharpExtensions.Tests.Kafka;

using System.Collections.Generic;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Any;
using Microsoft.OpenApi.Models;
using Xunit;

public sealed class KafkaMaintenanceParameterFilterTests
{
    [Fact]
    public void Apply_DefaultConfiguration_DoesNotExposeTopicKeys()
    {
        var options = new KafkaOptions
        {
            Topics = new Dictionary<string, KafkaTopicConfiguration>
            {
                ["SensitiveTopicKey"] = new() { TopicName = "events.test.changed.v1" }
            }
        };
        var filter = new KafkaMaintenanceParameterFilter(Options.Create(options));
        var parameter = new OpenApiParameter { Name = "topicConfigurationKey" };

        filter.Apply(parameter, null!);

        Assert.Empty(parameter.Schema.Enum);
        Assert.Equal("Kafka topic configuration key.", parameter.Description);
    }

    [Fact]
    public void Apply_ExplicitOptIn_ExposesOnlyBoundedSafeTopicKeys()
    {
        var options = new KafkaOptions
        {
            Topics = new Dictionary<string, KafkaTopicConfiguration>
            {
                ["Events.Test.V1"] = new() { TopicName = "events.test.changed.v1" },
                ["Commands.Test.V1"] = new() { TopicName = "commands.test.changed.v1" }
            }
        };
        options.Maintenance.ExposeTopicConfigurationKeysInOpenApi = true;
        var filter = new KafkaMaintenanceParameterFilter(Options.Create(options));
        var parameter = new OpenApiParameter { Name = "topicConfigurationKey" };

        filter.Apply(parameter, null!);

        Assert.Equal(2, parameter.Schema.Enum.Count);
        Assert.Equal("Commands.Test.V1", Assert.IsType<OpenApiString>(parameter.Schema.Enum[0]).Value);
        Assert.Equal("Events.Test.V1", Assert.IsType<OpenApiString>(parameter.Schema.Enum[1]).Value);
    }
}
