using CSharpExtensions.Foundation.Security.Pii;
using Xunit;

namespace CSharpExtensions.Tests
{
    public partial class GenericContainer<T>
        where T : class
    {
        [SensitiveData]
        public partial class NestedModel
        {
            [NonSensitiveProperty]
            public string Code { get; set; } = string.Empty;

            public string Unclassified { get; set; } = string.Empty;
        }
    }

    [SensitiveData]
    public partial class DuplicateName
    {
        [SensitiveProperty(SensitiveType.Text)]
        public string Secret { get; set; } = string.Empty;
    }

    [SensitiveData]
    public partial class LocalNameCollision
    {
        [SensitiveProperty(SensitiveType.Text)]
        public string builder { get; set; } = string.Empty;
    }

    public class PiiMaskingGeneratorShapeTests
    {
        [Fact]
        public void Generator_ShouldSupportNestedGenericTypesAndFailClosed()
        {
            var model = new GenericContainer<string>.NestedModel
            {
                Code = "public-code",
                Unclassified = "must-not-leak"
            };

            var result = model.Mask();

            Assert.Contains("Code = public-code", result);
            Assert.Contains("Unclassified = *****", result);
            Assert.DoesNotContain("must-not-leak", result);
        }

        [Fact]
        public void Generator_ShouldCreateUniqueSourcesForSameSimpleTypeName()
        {
            var first = new DuplicateName { Secret = "first-secret" };
            var second = new SecondaryNamespace.DuplicateName { Secret = "second-secret" };

            Assert.DoesNotContain("first-secret", first.Mask());
            Assert.DoesNotContain("second-secret", second.Mask());
        }

        [Fact]
        public void Generator_ShouldQualifyPropertiesThatCollideWithGeneratedLocals()
        {
            var model = new LocalNameCollision { builder = "secret-value" };

            var result = model.Mask();

            Assert.Contains("builder = *****", result);
            Assert.DoesNotContain("secret-value", result);
        }
    }
}

namespace SecondaryNamespace
{
    [SensitiveData]
    public partial class DuplicateName
    {
        [SensitiveProperty(SensitiveType.Text)]
        public string Secret { get; set; } = string.Empty;
    }
}