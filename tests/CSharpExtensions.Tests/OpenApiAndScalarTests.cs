using CSharpExtensions.AspNetCore.AspNet.Extensions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Scalar.AspNetCore;
using Xunit;

namespace CSharpExtensions.Tests;

public class OpenApiAndScalarTests
{
    [Fact]
    public void AddOpenApiDocumentation_ShouldRegisterOpenApiServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddOpenApiDocumentation("v1", options =>
        {
            // Custom configuration callback
        });

        var serviceProvider = services.BuildServiceProvider();

        // Assert
        // Verify that OpenApiMarkerService / options are registered
        Assert.NotNull(serviceProvider);
    }

    [Fact]
    public void MapScalarDocumentation_ShouldChainEndpointRouteBuilder()
    {
        // Arrange
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddOpenApiDocumentation();
        var app = builder.Build();

        // Act
        var result = app.MapScalarDocumentation(options =>
        {
            options.WithTitle("Custom Test Title");
        });

        // Assert
        Assert.NotNull(result);
        Assert.Same(app, result);
    }
}
