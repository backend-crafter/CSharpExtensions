namespace CSharpExtensions.Foundation.Security.Pii;

/// <summary>
/// Marks a class or record as containing sensitive data (PII) to trigger Roslyn Source Generation.
/// Classes marked with this attribute must be declared as partial.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
public sealed class SensitiveDataAttribute : Attribute
{
}
