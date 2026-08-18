using CSharpExtensions.Core.Security.Pii;
using CSharpExtensions.Security.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace CSharpExtensions.Tests;

public class PiiMaskingGeneratorDiagnosticTests
{
    [Fact]
    public void Generator_ShouldRejectExplicitToStringThatCanLeakPii()
    {
        const string source = """
            using CSharpExtensions.Core.Security.Pii;

            [SensitiveData]
            public partial class LeakingModel
            {
                public string Secret { get; set; } = string.Empty;
                public override string ToString() => Secret;
            }
            """;

        var result = RunGenerator(source, out _);

        Assert.Contains(result.Diagnostics, diagnostic => diagnostic.Id == "SP0006");
    }

    [Fact]
    public void Generator_ShouldRejectStaticSensitiveType()
    {
        const string source = """
            using CSharpExtensions.Core.Security.Pii;

            [SensitiveData]
            public static partial class StaticSensitiveModel
            {
                public static string Secret { get; set; } = string.Empty;
            }
            """;

        var result = RunGenerator(source, out _);

        Assert.Contains(result.Diagnostics, diagnostic => diagnostic.Id == "SP0005");
    }

    [Fact]
    public void Generator_ShouldRejectStaticParameterlessMaskCollision()
    {
        const string source = """
            using CSharpExtensions.Core.Security.Pii;

            [SensitiveData]
            public partial class StaticMaskModel
            {
                public string Secret { get; set; } = string.Empty;
                public static string Mask() => string.Empty;
            }
            """;

        var result = RunGenerator(source, out _);

        Assert.Contains(result.Diagnostics, diagnostic => diagnostic.Id == "SP0004");
    }

    [Fact]
    public void Generator_ShouldCompilePrimaryConstructorRecordShape()
    {
        const string source = """
            using System;
            using CSharpExtensions.Core.Security.Pii;

            [SensitiveData]
            public sealed partial record Profile(
                Guid UserId,
                int CurrentLevel,
                DateTimeOffset UpdatedAt);
            """;

        var result = RunGenerator(source, out var outputCompilation);

        Assert.DoesNotContain(result.Diagnostics, diagnostic => diagnostic.Severity == DiagnosticSeverity.Error);
        Assert.DoesNotContain(
            outputCompilation.GetDiagnostics(),
            diagnostic => diagnostic.Severity == DiagnosticSeverity.Error);
    }

    private static GeneratorDriverRunResult RunGenerator(
        string source,
        out Compilation outputCompilation)
    {
        var platformAssemblies = (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")
            ?? throw new InvalidOperationException("Trusted platform assemblies are unavailable.");
        var references = platformAssemblies
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Select(path => MetadataReference.CreateFromFile(path))
            .Append(MetadataReference.CreateFromFile(typeof(SensitiveDataAttribute).Assembly.Location));
        var syntaxTree = CSharpSyntaxTree.ParseText(
            source,
            new CSharpParseOptions(LanguageVersion.Latest));
        var compilation = CSharpCompilation.Create(
            $"GeneratorTest_{Guid.NewGuid():N}",
            [syntaxTree],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            new PiiMaskingGenerator().AsSourceGenerator());

        driver = driver.RunGeneratorsAndUpdateCompilation(
            compilation,
            out outputCompilation,
            out _);
        return driver.GetRunResult();
    }
}
