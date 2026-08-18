using CSharpExtensions.Foundation.Json.Enums;
using CSharpExtensions.Foundation.Json.Extensions;
using CSharpExtensions.Foundation.Railway.Extensions;
using Xunit;

namespace CSharpExtensions.Tests;

public class JsonTests
{
    private record TestModel(string FirstName, int AgeCount, string? OptionalValue = null);

    [Fact]
    public void ToJson_ShouldUseCamelCase()
    {
        // Arrange
        var model = new TestModel("John", 30);

        // Act
        var json = model.ToJson();

        // Assert
        Assert.Contains("\"firstName\":\"John\"", json);
        Assert.Contains("\"ageCount\":30", json);
        Assert.DoesNotContain("OptionalValue", json);
    }

    [Fact]
    public void FromJson_ShouldReturnSuccessfulResult()
    {
        // Arrange
        var json = "{\"firstName\":\"John\",\"ageCount\":30}";

        // Act
        var result = json.TryDeserialize<TestModel>();

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal("John", result.Value.FirstName);
        Assert.Equal(30, result.Value.AgeCount);
    }

    [Fact]
    public void TryDeserialize_ShouldReturnTrueForValidJson()
    {
        // Arrange
        var json = "{\"firstName\":\"John\",\"ageCount\":30}";

        // Act
        var success = json.TryDeserialize<TestModel>(out var model);

        // Assert
        Assert.True(success);
        Assert.NotNull(model);
        Assert.Equal("John", model!.FirstName);
    }

    [Fact]
    public void FromJson_WithInvalidJson_ShouldReturnFailure()
    {
        // Arrange
        var json = "{\"firstName\": John}"; // Missing quotes for value

        // Act
        var result = json.TryDeserialize<TestModel>();

        // Assert
        Assert.True(result.IsFailure);
        Assert.Equal("JsonSerializationError", result.Error.Type);
        Assert.Equal(400, result.Error.HttpStatusCode);
    }

    [Fact]
    public void FromUtf8JsonSafe_WithInvalidStructure_ShouldReturnFailureWithoutException()
    {
        // Arrange
        var invalidJson = "{\"name\": \"John\""u8; // Missing closing brace

        // Act
        var result = invalidJson.TryDeserializeSafe<TestModel>();

        // Assert
        Assert.True(result.IsFailure);
        Assert.Equal("JsonSafeSerializationError", result.Error.Type);
    }

    [Fact]
    public void ROP_Chaining_ShouldWork()
    {
        // Arrange
        var json = "{\"firstName\":\"John\",\"ageCount\":30}";

        // Act
        var result = json.TryDeserialize<TestModel>()
            .Transform(m => m.FirstName.ToUpper())
            .Match(
                name => name,
                error => "Error"
            );

        // Assert
        Assert.Equal("JOHN", result);
    }

    [Fact]
    public void Merge_DeepMergeObjects_ShouldWork()
    {
        // Arrange
        var target = "{\"name\":\"John\",\"meta\":{\"age\":30,\"city\":\"New York\"}}";
        var source = "{\"meta\":{\"age\":31},\"hobby\":\"Coding\"}";

        // Act
        var result = JsonExtensions.Merge(target, source);

        // Assert
        Assert.Contains("\"name\":\"John\"", result);
        Assert.Contains("\"age\":31", result);
        Assert.Contains("\"city\":\"New York\"", result);
        Assert.Contains("\"hobby\":\"Coding\"", result);
    }

    [Fact]
    public void Merge_ArrayReplace_ShouldWork()
    {
        // Arrange
        var target = "{\"tags\":[\"a\",\"b\"]}";
        var source = "{\"tags\":[\"c\"]}";

        // Act
        var result = JsonExtensions.Merge(target, source, JsonMergeHandling.Replace);

        // Assert
        Assert.Equal("{\"tags\":[\"c\"]}", result);
    }

    [Fact]
    public void Merge_ArrayConcat_ShouldWork()
    {
        // Arrange
        var target = "{\"tags\":[\"a\"]}";
        var source = "{\"tags\":[\"b\"]}";

        // Act
        var result = JsonExtensions.Merge(target, source, JsonMergeHandling.Concat);

        // Assert
        Assert.Equal("{\"tags\":[\"a\",\"b\"]}", result);
    }

    [Fact]
    public void Merge_ArrayUnion_ShouldWork()
    {
        // Arrange
        var target = "{\"tags\":[\"a\",\"b\"]}";
        var source = "{\"tags\":[\"b\",\"c\"]}";

        // Act
        var result = JsonExtensions.Merge(target, source, JsonMergeHandling.Union);

        // Assert
        Assert.Equal("{\"tags\":[\"a\",\"b\",\"c\"]}", result);
    }

    [Fact]
    public void Merge_ArrayMerge_ShouldWork()
    {
        // Arrange
        var target = "{\"users\":[{\"id\":1,\"name\":\"John\"}]}";
        var source = "{\"users\":[{\"name\":\"John Doe\"},{\"id\":2,\"name\":\"Jane\"}]}";

        // Act
        var result = JsonExtensions.Merge(target, source, JsonMergeHandling.Merge);

        // Assert
        Assert.Contains("\"id\":1", result);
        Assert.Contains("\"name\":\"John Doe\"", result);
        Assert.Contains("\"id\":2", result);
        Assert.Contains("\"name\":\"Jane\"", result);
    }
}
