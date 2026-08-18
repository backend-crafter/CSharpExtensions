using System.Text.Json;
using CSharpExtensions.Core.Json;
using Xunit;

namespace CSharpExtensions.Tests;

public class JsonOptionsTests
{
    [Fact]
    public void Default_ShouldUseCamelCase()
    {
        var options = JsonOptions.Default;
        Assert.Equal(JsonNamingPolicy.CamelCase, options.PropertyNamingPolicy);
    }

    [Fact]
    public void SnakeCase_ShouldUseSnakeCase()
    {
        var options = JsonOptions.SnakeCase;
        // Verify naming policy works
        var name = options.PropertyNamingPolicy?.ConvertName("FirstName");
        Assert.Equal("first_name", name);
    }

    [Fact]
    public void KebabCase_ShouldUseKebabCase()
    {
        var options = JsonOptions.KebabCase;
        var name = options.PropertyNamingPolicy?.ConvertName("FirstName");
        Assert.Equal("first-name", name);
    }
}
