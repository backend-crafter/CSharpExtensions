namespace CSharpExtensions.Kafka.Abstractions;

using System;

/// <summary>
/// Marks a property as the assembly key inside a composite context or event payload.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class AssemblyKeyAttribute : Attribute
{
}
