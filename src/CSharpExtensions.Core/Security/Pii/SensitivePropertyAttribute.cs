namespace CSharpExtensions.Core.Security.Pii;

/// <summary>
/// Marks a property as containing sensitive data (PII) to be masked during logging or string transformation.
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class SensitivePropertyAttribute : Attribute
{
    /// <summary>
    /// Gets the type of sensitive data.
    /// </summary>
    public SensitiveType Type { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="SensitivePropertyAttribute"/> class.
    /// </summary>
    /// <param name="type">The type of sensitive data for masking rules.</param>
    public SensitivePropertyAttribute(SensitiveType type = SensitiveType.Text)
    {
        Type = type;
    }
}
