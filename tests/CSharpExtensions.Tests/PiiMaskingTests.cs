using CSharpExtensions.Foundation.Security.Pii;
using Xunit;

namespace CSharpExtensions.Tests;

[SensitiveData]
public partial class TestUserModel
{
    [NonSensitiveProperty]
    public int Id { get; set; }
    
    public string Name { get; set; } = string.Empty;

    [SensitiveProperty(SensitiveType.Phone)]
    public string Phone { get; set; } = string.Empty;

    [SensitiveProperty(SensitiveType.Email)]
    public string Email { get; set; } = string.Empty;

    [SensitiveProperty(SensitiveType.Card)]
    public string CardNumber { get; set; } = string.Empty;

    [SensitiveProperty(SensitiveType.Text)]
    public string SecretText { get; set; } = string.Empty;
}

public class PiiMaskingTests
{
    [Fact]
    public void RedactPhone_OversizedInput_ShouldReturnFixedPlaceholder()
    {
        var result = new string('1', 10_000).RedactPhone();

        Assert.Equal("*****", result);
    }

    [Fact]
    public void MaskCard_OversizedInput_ShouldReturnFixedPlaceholder()
    {
        var result = SensitiveDataMasker.Mask(new string('1', 10_000), SensitiveType.Card);

        Assert.Equal("*****", result);
    }

    [Fact]
    public void MaskEmail_MultipleSeparators_ShouldUseBoundedFallback()
    {
        var result = ("a" + new string('@', 10_000) + "b").MaskEmail();

        Assert.Equal("***@***", result);
    }

    [Fact]
    public void Mask_ShouldCorrectlyMaskPIIFields()
    {
        var model = new TestUserModel
        {
            Id = 123,
            Name = "John Doe",
            Phone = "+37499123456",
            Email = "john.doe@gmail.com",
            CardNumber = "1234567890123456",
            SecretText = "SuperSecretPassword"
        };

        var maskedString = model.Mask();
        var toStringResult = model.ToString();

        Assert.Equal(maskedString, toStringResult);
        
        // Logging redaction keeps only the last two phone digits.
        Assert.Contains("Phone = **********56", maskedString);

        // Logging redaction never exposes the local part or domain.
        Assert.Contains("Email = ***@***", maskedString);

        // Assert Card is masked: keep last 4
        // "************3456"
        Assert.Contains("CardNumber = ************3456", maskedString);

        // Assert Text is masked: all replacement
        // "*****"
        Assert.Contains("SecretText = *****", maskedString);

        // Explicitly classified non-sensitive fields remain visible.
        Assert.Contains("Id = 123", maskedString);

        // Unclassified fields fail closed.
        Assert.Contains("Name = *****", maskedString);
        Assert.DoesNotContain("John Doe", maskedString);
    }
}
