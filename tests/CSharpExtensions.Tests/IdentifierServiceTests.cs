using CSharpExtensions.Core.Security.Options;
using CSharpExtensions.Core.Security.Services;
using Microsoft.Extensions.Options;
using Xunit;

namespace CSharpExtensions.Tests;

public class IdentifierServiceTests
{
    private readonly SqidsIdentifierService _service;
    private readonly IdentifierOptions _options;

    public IdentifierServiceTests()
    {
        _options = new IdentifierOptions();
        var optionsMock = Options.Create(_options);
        _service = new SqidsIdentifierService(optionsMock);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(123456789)]
    [InlineData(long.MaxValue)]
    public void EncodeAndDecode_ShouldReturnOriginalId(long originalId)
    {
        // Act
        var encoded = _service.Encode(originalId);
        var decoded = _service.Decode(encoded);

        // Assert
        Assert.Equal(originalId, decoded);
    }

    [Fact]
    public void Encode_ShouldProduceMinimumLengthString()
    {
        // Arrange
        const long id = 1;

        // Act
        var encoded = _service.Encode(id);

        // Assert
        Assert.True(encoded.Length >= _options.MinLength);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(100)]
    [InlineData(1000000)]
    public void Encode_ShouldNotContainExcludedCharacters(long id)
    {
        // Arrange
        var excludedCharacters = new[] { '0', 'O', '1', 'l', 'I' };

        // Act
        var encoded = _service.Encode(id);

        // Assert
        foreach (var excluded in excludedCharacters)
        {
            Assert.DoesNotContain(excluded, encoded);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("invalid-chars-!@#")]
    public void Decode_WithInvalidInput_ShouldReturnNull(string invalidInput)
    {
        // Act
        var result = _service.Decode(invalidInput);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void Decode_WithNullInput_ShouldReturnNull()
    {
        // Act
        var result = _service.Decode(null!);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void Decode_WithUnboundedInput_ShouldReturnNull()
    {
        Assert.Null(_service.Decode(new string('a', 257)));
    }

    [Fact]
    public void Constructor_WithDuplicateAlphabet_ShouldFailValidation()
    {
        var options = Options.Create(new IdentifierOptions
        {
            Alphabet = "aabc",
            MinLength = 8
        });

        Assert.Throws<OptionsValidationException>(() => new SqidsIdentifierService(options));
    }
}
