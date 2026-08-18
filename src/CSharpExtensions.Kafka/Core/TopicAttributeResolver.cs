namespace CSharpExtensions.Kafka.Core;

using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

/// <summary>
/// Helper utility to resolve the configuration key (topic alias) from message types.
/// Supports duck-typing to decouple contract assemblies from referencing the tools assembly.
/// </summary>
public static class TopicAttributeResolver
{
    /// <summary>
    /// Resolves the configuration key for the specified type.
    /// Looks for public members in order: MessageType, Domain, Aggregate, Action, Version.
    /// Falls back to attributes or type name.
    /// </summary>
    public static string Resolve(Type type)
    {
        if (type is null) throw new ArgumentNullException(nameof(type));

        // 1. Try resolving using new convention (constants/properties in class)
        if (TryGetConventionMetadata(type, out var domain, out var messageType, out var aggregate, out var action, out var version))
        {
            return $"{messageType}{domain}{aggregate}{action}V{version}";
        }

        // 2. Try resolving using [Topic] attribute (backward compatibility)
        var attributes = type.GetCustomAttributes(inherit: true);
        foreach (var attribute in attributes)
        {
            var attrType = attribute.GetType();
            if (attrType.Name == "TopicAttribute" || attrType.FullName == "CSharpExtensions.Kafka.Abstractions.TopicAttribute")
            {
                var property = attrType.GetProperty("ConfigurationKey");
                if (property is not null)
                {
                    var value = property.GetValue(attribute) as string;
                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        return value;
                    }
                }
            }
        }

        return type.Name;
    }

    /// <summary>
    /// Resolves the configuration key for the specified type.
    /// </summary>
    public static string Resolve<T>()
    {
        return Resolve(typeof(T));
    }

    /// <summary>
    /// Resolves the physical topic name based on type metadata or configured name.
    /// </summary>
    public static string ResolveTopicName(Type type, string configuredTopicName = "")
    {
        if (!string.IsNullOrWhiteSpace(configuredTopicName))
        {
            return configuredTopicName;
        }

        if (type is null) throw new ArgumentNullException(nameof(type));

        if (TryGetConventionMetadata(type, out var domain, out var messageType, out var aggregate, out var action, out var version))
        {
            return $"{messageType.ToLowerInvariant()}.{domain.ToLowerInvariant()}.{ToKebabCase(aggregate)}.{action.ToLowerInvariant()}.v{version}";
        }

        return ToKebabCase(type.Name);
    }

    /// <summary>
    /// Resolves the physical topic name based on type metadata or configured name.
    /// </summary>
    public static string ResolveTopicName<T>(string configuredTopicName = "")
    {
        return ResolveTopicName(typeof(T), configuredTopicName);
    }

    private static MemberInfo? GetMetadataFieldOrProperty(Type type, string name)
    {
        var field = type.GetField(name, BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
        if (field is not null) return field;
        
        var property = type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.FlattenHierarchy);
        return property;
    }

    private static bool TryGetConventionMetadata(Type type, out string domain, out string messageType, out string aggregate, out string action, out int version)
    {
        domain = "";
        messageType = "";
        aggregate = "";
        action = "";
        version = 1;

        var messageTypeMember = GetMetadataFieldOrProperty(type, "MessageType");
        var domainMember = GetMetadataFieldOrProperty(type, "Domain");
        var aggregateMember = GetMetadataFieldOrProperty(type, "Aggregate");
        var actionMember = GetMetadataFieldOrProperty(type, "Action");
        var versionMember = GetMetadataFieldOrProperty(type, "Version");

        if (domainMember is null || messageTypeMember is null || aggregateMember is null || actionMember is null || versionMember is null)
        {
            return false;
        }

        object? instance = null;
        if (RequiresInstance(messageTypeMember)
            || RequiresInstance(domainMember)
            || RequiresInstance(aggregateMember)
            || RequiresInstance(actionMember)
            || RequiresInstance(versionMember))
        {
            try
            {
                instance = RuntimeHelpers.GetUninitializedObject(type);
            }
            catch (Exception exception) when (exception is ArgumentException or MemberAccessException or NotSupportedException)
            {
                throw new InvalidOperationException(
                    $"Kafka topic metadata for type '{type.FullName}' cannot be read without invoking its constructor.");
            }
        }

        try
        {
            messageType = GetMemberValue(messageTypeMember, instance) as string ?? "";
            domain = GetMemberValue(domainMember, instance) as string ?? "";
            aggregate = GetMemberValue(aggregateMember, instance) as string ?? "";
            action = GetMemberValue(actionMember, instance) as string ?? "";

            if (GetMemberValue(versionMember, instance) is int declaredVersion)
            {
                version = declaredVersion;
            }
        }
        catch (Exception exception) when (exception is TargetInvocationException or MethodAccessException)
        {
            throw new InvalidOperationException(
                $"Kafka topic metadata for type '{type.FullName}' could not be evaluated safely.");
        }

        if (string.IsNullOrWhiteSpace(messageType)
            || string.IsNullOrWhiteSpace(domain)
            || string.IsNullOrWhiteSpace(aggregate)
            || string.IsNullOrWhiteSpace(action)
            || version is < 1 or > 1000)
        {
            throw new InvalidOperationException(
                $"Kafka topic metadata for type '{type.FullName}' is incomplete or invalid.");
        }

        return true;
    }

    private static bool RequiresInstance(MemberInfo member)
    {
        return member is PropertyInfo { GetMethod.IsStatic: false };
    }

    private static object? GetMemberValue(MemberInfo member, object? instance)
    {
        if (member is FieldInfo field)
        {
            if (field.IsLiteral)
            {
                return field.GetRawConstantValue();
            }
            return field.GetValue(null);
        }
        if (member is PropertyInfo property)
        {
            return property.GetValue(property.GetMethod?.IsStatic == true ? null : instance);
        }
        return null;
    }

    private static string ToKebabCase(string value)
    {
        if (string.IsNullOrEmpty(value)) return "";
        return Regex.Replace(
            value,
            "(?<!^)([A-Z][a-z]|(?<=[a-z])[A-Z])",
            "-$1",
            RegexOptions.Compiled
        ).Trim().ToLowerInvariant();
    }
}
