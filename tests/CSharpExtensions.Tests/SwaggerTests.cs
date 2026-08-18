using System.Reflection;
using CSharpExtensions.AspNetCore.AspNet.Extensions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.Swagger;
using Swashbuckle.AspNetCore.SwaggerGen;
using Moq;
using Xunit;

namespace CSharpExtensions.Tests;

[ApiExplorerSettings(GroupName = "Zeta v1")]
public class TestZetaController : ControllerBase
{
    [HttpGet("zeta")]
    public IActionResult Get() => Ok();
}

[ApiExplorerSettings(GroupName = "Alpha v1")]
public class TestAlphaController : ControllerBase
{
    [HttpGet("alpha")]
    public IActionResult Get() => Ok();
}

public class SwaggerTests
{
    [Fact]
    public void AddSwaggerDocumentation_ShouldConfigureStandardHttpBearerSecurity()
    {
        // Arrange
        var services = new ServiceCollection();
        // We need these for SwaggerGen to work
        services.AddLogging();
        services.AddMvc(); 
        
        var hostEnvironmentMock = new Mock<Microsoft.AspNetCore.Hosting.IWebHostEnvironment>();
        hostEnvironmentMock.Setup(x => x.ApplicationName).Returns("TestApp");
        services.AddSingleton<Microsoft.AspNetCore.Hosting.IWebHostEnvironment>(hostEnvironmentMock.Object);
        services.AddSingleton<Microsoft.Extensions.Hosting.IHostEnvironment>(hostEnvironmentMock.Object);
        
        var assembly = Assembly.GetExecutingAssembly();

        // Act
        services.AddSwaggerDocumentation(assembly);
        var serviceProvider = services.BuildServiceProvider();
        
        // Swashbuckle generates the document through ISwaggerProvider
        var swaggerProvider = serviceProvider.GetRequiredService<ISwaggerProvider>();
        var document = swaggerProvider.GetSwagger("Alpha v1");

        // Assert
        Assert.NotNull(document.Components.SecuritySchemes);
        Assert.True(document.Components.SecuritySchemes.ContainsKey("Bearer"));
        
        var scheme = document.Components.SecuritySchemes["Bearer"];
        Assert.Equal("JWT bearer access token.", scheme.Description);
        Assert.Equal(SecuritySchemeType.Http, scheme.Type);
        Assert.Equal("bearer", scheme.Scheme);
        Assert.Equal("JWT", scheme.BearerFormat);
        Assert.Null(scheme.Name);
    }

    [Fact]
    public void AddSwaggerDocumentation_ShouldOrderGroupsAlphabetically()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMvc();
        var hostEnvironmentMock = new Mock<Microsoft.AspNetCore.Hosting.IWebHostEnvironment>();
        hostEnvironmentMock.Setup(x => x.ApplicationName).Returns("TestApp");
        services.AddSingleton<Microsoft.AspNetCore.Hosting.IWebHostEnvironment>(hostEnvironmentMock.Object);
        services.AddSingleton<Microsoft.Extensions.Hosting.IHostEnvironment>(hostEnvironmentMock.Object);

        var assembly = Assembly.GetExecutingAssembly();

        // Act
        services.AddSwaggerDocumentation(assembly);
        var serviceProvider = services.BuildServiceProvider();
        var swaggerGenOptions = serviceProvider.GetRequiredService<IOptions<SwaggerGenOptions>>().Value;

        // Assert
        var docKeys = swaggerGenOptions.SwaggerGeneratorOptions.SwaggerDocs.Keys.ToList();
        Assert.Contains("Alpha v1", docKeys);
        Assert.Contains("Zeta v1", docKeys);
        var alphaIndex = docKeys.IndexOf("Alpha v1");
        var zetaIndex = docKeys.IndexOf("Zeta v1");
        Assert.True(alphaIndex < zetaIndex, "Alpha v1 should appear before Zeta v1");
    }
}
