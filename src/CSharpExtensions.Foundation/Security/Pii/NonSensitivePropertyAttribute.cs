namespace CSharpExtensions.Foundation.Security.Pii;

/// <summary>
/// Explicitly allows a property on a <see cref="SensitiveDataAttribute"/> type to appear in masked diagnostics.
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class NonSensitivePropertyAttribute : Attribute
{
}
