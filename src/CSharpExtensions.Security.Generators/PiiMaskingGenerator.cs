using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace CSharpExtensions.Security.Generators;

/// <summary>
/// Generates fail-safe PII masking for types marked with SensitiveDataAttribute.
/// </summary>
[Generator]
public sealed class PiiMaskingGenerator : IIncrementalGenerator
{
    private const string SensitiveDataAttributeName =
        "CSharpExtensions.Security.Pii.SensitiveDataAttribute";
    private const string SensitivePropertyAttributeName =
        "CSharpExtensions.Security.Pii.SensitivePropertyAttribute";
    private const string NonSensitivePropertyAttributeName =
        "CSharpExtensions.Security.Pii.NonSensitivePropertyAttribute";

    private const string CoreSensitiveDataAttributeName =
        "CSharpExtensions.Foundation.Security.Pii.SensitiveDataAttribute";
    private const string CoreSensitivePropertyAttributeName =
        "CSharpExtensions.Foundation.Security.Pii.SensitivePropertyAttribute";
    private const string CoreNonSensitivePropertyAttributeName =
        "CSharpExtensions.Foundation.Security.Pii.NonSensitivePropertyAttribute";

    private static readonly DiagnosticDescriptor PartialTypeRequired = new(
        "SP0001",
        "Type must be partial",
        "The type '{0}' and each containing type must be partial to generate PII masking",
        "Security",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor UnsupportedSensitiveProperty = new(
        "SP0002",
        "Sensitive property type is unsupported",
        "Sensitive property '{0}' on '{1}' must be a string",
        "Security",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ConflictingClassification = new(
        "SP0003",
        "Property has conflicting PII classification",
        "Property '{0}' on '{1}' cannot be both sensitive and non-sensitive",
        "Security",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ExistingMaskMethod = new(
        "SP0004",
        "Mask method already exists",
        "The type '{0}' already declares a parameterless Mask method",
        "Security",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor UnsupportedTypeShape = new(
        "SP0005",
        "Type shape is unsupported",
        "The type '{0}' cannot be extended safely by the PII masking generator",
        "Security",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor ExistingToStringMethod = new(
        "SP0006",
        "ToString method can bypass PII masking",
        "The type '{0}' declares ToString and can bypass generated PII masking",
        "Security",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    /// <inheritdoc />
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var legacySensitiveTypes = context.SyntaxProvider.ForAttributeWithMetadataName(
            SensitiveDataAttributeName,
            static (node, _) => node is TypeDeclarationSyntax,
            static (attributeContext, _) => (INamedTypeSymbol)attributeContext.TargetSymbol);

        var coreSensitiveTypes = context.SyntaxProvider.ForAttributeWithMetadataName(
            CoreSensitiveDataAttributeName,
            static (node, _) => node is TypeDeclarationSyntax,
            static (attributeContext, _) => (INamedTypeSymbol)attributeContext.TargetSymbol);

        var sensitiveTypes = legacySensitiveTypes.Collect()
            .Combine(coreSensitiveTypes.Collect())
            .SelectMany(static (tuple, _) => tuple.Left.Concat(tuple.Right).Distinct<INamedTypeSymbol>(SymbolEqualityComparer.Default));

        context.RegisterSourceOutput(sensitiveTypes, static (productionContext, type) =>
            Generate(productionContext, type));
    }

    private static void Generate(SourceProductionContext context, INamedTypeSymbol type)
    {
        if (!IsSupportedType(type) || IsFileLocal(type))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                UnsupportedTypeShape,
                type.Locations.FirstOrDefault(),
                type.ToDisplayString()));
            return;
        }

        for (var current = type; current is not null; current = current.ContainingType)
        {
            if (!IsPartial(current))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    PartialTypeRequired,
                    current.Locations.FirstOrDefault(),
                    current.ToDisplayString()));
                return;
            }
        }

        if (type.GetMembers("Mask").OfType<IMethodSymbol>().Any(method =>
                method.Parameters.Length == 0))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                ExistingMaskMethod,
                type.Locations.FirstOrDefault(),
                type.ToDisplayString()));
            return;
        }

        if (type.GetMembers("ToString").OfType<IMethodSymbol>().Any(method =>
                !method.IsImplicitlyDeclared && !method.IsStatic && method.Parameters.Length == 0))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                ExistingToStringMethod,
                type.Locations.FirstOrDefault(),
                type.ToDisplayString()));
            return;
        }

        var properties = new List<PropertyModel>();
        foreach (var property in type.GetMembers().OfType<IPropertySymbol>())
        {
            if (property.IsStatic || property.IsIndexer ||
                property.DeclaredAccessibility != Accessibility.Public)
            {
                continue;
            }

            var sensitiveAttribute = property.GetAttributes().FirstOrDefault(attribute =>
                attribute.AttributeClass?.ToDisplayString() == SensitivePropertyAttributeName ||
                attribute.AttributeClass?.ToDisplayString() == CoreSensitivePropertyAttributeName);
            var nonSensitiveAttribute = property.GetAttributes().FirstOrDefault(attribute =>
                attribute.AttributeClass?.ToDisplayString() == NonSensitivePropertyAttributeName ||
                attribute.AttributeClass?.ToDisplayString() == CoreNonSensitivePropertyAttributeName);

            if (sensitiveAttribute is not null && nonSensitiveAttribute is not null)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    ConflictingClassification,
                    property.Locations.FirstOrDefault(),
                    property.Name,
                    type.ToDisplayString()));
                return;
            }

            if (sensitiveAttribute is not null && property.Type.SpecialType != SpecialType.System_String)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    UnsupportedSensitiveProperty,
                    property.Locations.FirstOrDefault(),
                    property.Name,
                    type.ToDisplayString()));
                return;
            }

            properties.Add(new PropertyModel(
                EscapeIdentifier(property.Name),
                property.Name,
                GetMaskType(sensitiveAttribute),
                nonSensitiveAttribute is not null));
        }

        var source = GenerateSource(type, properties);
        context.AddSource(CreateHintName(type), SourceText.From(source, Encoding.UTF8));
    }

    private static string GenerateSource(INamedTypeSymbol type, IReadOnlyList<PropertyModel> properties)
    {
        var builder = new StringBuilder();
        builder.AppendLine("// <auto-generated />");
        builder.AppendLine("#nullable enable");

        if (!type.ContainingNamespace.IsGlobalNamespace)
        {
            builder.Append("namespace ")
                .Append(type.ContainingNamespace.ToDisplayString())
                .AppendLine(";")
                .AppendLine();
        }

        var containingTypes = GetContainingTypes(type);
        foreach (var containingType in containingTypes)
        {
            AppendTypeDeclaration(builder, containingType);
            builder.AppendLine("{");
        }

        AppendTypeDeclaration(builder, type);
        builder.AppendLine("{");
        builder.AppendLine("    /// <summary>");
        builder.AppendLine("    /// Returns a representation in which unclassified and sensitive properties are redacted.");
        builder.AppendLine("    /// </summary>");
        builder.AppendLine("    public string Mask()");
        builder.AppendLine("    {");
        builder.AppendLine("        var builder = new global::System.Text.StringBuilder();");
        builder.Append("        builder.Append(\"")
            .Append(EscapeString(type.Name))
            .AppendLine(" { \");");

        for (var index = 0; index < properties.Count; index++)
        {
            var property = properties[index];
            builder.Append("        builder.Append(\"")
                .Append(EscapeString(property.DisplayName))
                .AppendLine(" = \");");

            if (property.IsExplicitlyNonSensitive)
            {
                builder.Append("        builder.Append(")
                    .Append("this.")
                    .Append(property.Identifier)
                    .AppendLine(");");
            }
            else if (property.MaskType is not null)
            {
                builder.Append("        builder.Append(global::CSharpExtensions.Foundation.Security.Pii.SensitiveDataMasker.Mask(")
                    .Append("this.")
                    .Append(property.Identifier)
                    .Append(", global::CSharpExtensions.Foundation.Security.Pii.SensitiveType.")
                    .Append(property.MaskType)
                    .AppendLine("));");
            }
            else
            {
                builder.AppendLine("        builder.Append(\"*****\");");
            }

            if (index < properties.Count - 1)
            {
                builder.AppendLine("        builder.Append(\", \");");
            }
            else
            {
                builder.AppendLine("        builder.Append(' ');");
            }
        }

        builder.AppendLine("        builder.Append('}');");
        builder.AppendLine("        return builder.ToString();");
        builder.AppendLine("    }");

        builder.AppendLine();
        builder.AppendLine("    /// <inheritdoc />");
        builder.AppendLine("    public override string ToString() => Mask();");

        builder.AppendLine("}");
        for (var index = 0; index < containingTypes.Count; index++)
        {
            builder.AppendLine("}");
        }

        return builder.ToString();
    }

    private static void AppendTypeDeclaration(StringBuilder builder, INamedTypeSymbol type)
    {
        var accessibility = GetAccessibility(type.DeclaredAccessibility);
        if (accessibility.Length > 0)
        {
            builder.Append(accessibility).Append(' ');
        }

        if (type.TypeKind == TypeKind.Struct && type.IsReadOnly)
        {
            builder.Append("readonly ");
        }

        if (type.IsRefLikeType)
        {
            builder.Append("ref ");
        }

        builder.Append("partial ").Append(GetTypeKeyword(type)).Append(' ')
            .Append(EscapeIdentifier(type.Name));

        if (type.TypeParameters.Length > 0)
        {
            builder.Append('<');
            for (var index = 0; index < type.TypeParameters.Length; index++)
            {
                if (index > 0)
                {
                    builder.Append(", ");
                }

                builder.Append(EscapeIdentifier(type.TypeParameters[index].Name));
            }

            builder.Append('>');
        }

        builder.AppendLine();
    }

    private static IReadOnlyList<INamedTypeSymbol> GetContainingTypes(INamedTypeSymbol type)
    {
        var result = new List<INamedTypeSymbol>();
        for (var current = type.ContainingType; current is not null; current = current.ContainingType)
        {
            result.Add(current);
        }

        result.Reverse();
        return result;
    }

    private static bool IsSupportedType(INamedTypeSymbol type)
        => !type.IsStatic && type.TypeKind is (TypeKind.Class or TypeKind.Struct);

    private static bool IsPartial(INamedTypeSymbol type)
        => type.DeclaringSyntaxReferences.Any(reference =>
            reference.GetSyntax() is TypeDeclarationSyntax declaration &&
            declaration.Modifiers.Any(modifier => modifier.IsKind(SyntaxKind.PartialKeyword)));

    private static bool IsFileLocal(INamedTypeSymbol type)
        => type.DeclaringSyntaxReferences.Any(reference =>
            reference.GetSyntax() is TypeDeclarationSyntax declaration &&
            declaration.Modifiers.Any(modifier => modifier.ValueText == "file"));

    private static string? GetMaskType(AttributeData? attribute)
    {
        if (attribute is null)
        {
            return null;
        }

        if (attribute.ConstructorArguments.Length == 0 ||
            attribute.ConstructorArguments[0].Value is not int value)
        {
            return "Text";
        }

        return value switch
        {
            0 => "Text",
            1 => "Phone",
            2 => "Email",
            3 => "Card",
            _ => "Text"
        };
    }

    private static string GetTypeKeyword(INamedTypeSymbol type)
    {
        if (type.IsRecord)
        {
            return type.TypeKind == TypeKind.Struct ? "record struct" : "record";
        }

        return type.TypeKind == TypeKind.Struct ? "struct" : "class";
    }

    private static string GetAccessibility(Accessibility accessibility)
        => accessibility switch
        {
            Accessibility.Public => "public",
            Accessibility.Internal => "internal",
            Accessibility.Private => "private",
            Accessibility.Protected => "protected",
            Accessibility.ProtectedAndInternal => "private protected",
            Accessibility.ProtectedOrInternal => "protected internal",
            _ => string.Empty
        };

    private static string EscapeIdentifier(string identifier)
        => SyntaxFacts.GetKeywordKind(identifier) != SyntaxKind.None ? "@" + identifier : identifier;

    private static string EscapeString(string value)
        => value.Replace("\\", "\\\\").Replace("\"", "\\\"");

    private static string CreateHintName(INamedTypeSymbol type)
    {
        var identity = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        var sanitized = new string(identity.Select(character =>
            char.IsLetterOrDigit(character) ? character : '_').ToArray());
        return $"{sanitized}_{ComputeStableHash(identity):x16}_Masked.g.cs";
    }

    private static ulong ComputeStableHash(string value)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        var hash = offset;

        foreach (var character in value)
        {
            hash ^= character;
            hash *= prime;
        }

        return hash;
    }

    private sealed class PropertyModel
    {
        internal PropertyModel(
            string identifier,
            string displayName,
            string? maskType,
            bool isExplicitlyNonSensitive)
        {
            Identifier = identifier;
            DisplayName = displayName;
            MaskType = maskType;
            IsExplicitlyNonSensitive = isExplicitlyNonSensitive;
        }

        internal string Identifier { get; }
        internal string DisplayName { get; }
        internal string? MaskType { get; }
        internal bool IsExplicitlyNonSensitive { get; }
    }
}
