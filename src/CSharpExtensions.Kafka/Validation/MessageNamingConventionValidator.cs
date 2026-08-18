namespace CSharpExtensions.Kafka.Validation;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

/// <summary>
/// Validates that message types follow naming conventions at startup.
/// Enforces: userId (Client/User), employeeId (Employee/Staff), presence of TenantId/PartnerId, and strict camelCase naming.
/// Prohibits legacy terms (e.g. playerId, memberId, clientId).
/// </summary>
public static class MessageNamingConventionValidator
{
    private static readonly Regex ProhibitedPropertyRegex = new(
        @"(?i)player|(?i)member|^(?!UserId$|Users$|EmployeeId$|Employees$|.*UserAgent.*).*User.*|^(?i)client_?id$",
        RegexOptions.Compiled);

    /// <summary>
    /// Validates a registered message type for naming convention violations.
    /// Throws InvalidOperationException at startup if violations are found.
    /// </summary>
    /// <typeparam name="TMessage">The message type to validate.</typeparam>
    public static void Validate<TMessage>()
    {
        Validate(typeof(TMessage));
    }

    /// <summary>
    /// Validates a type for naming convention violations.
    /// </summary>
    /// <param name="messageType">The message type to validate.</param>
    /// <exception cref="ArgumentNullException">Thrown when messageType is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when naming convention violations are detected.</exception>
    public static void Validate(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        var interfaces = messageType.GetInterfaces();
        var isUserCommand = interfaces.Any(i => i.Name == "IUserCommand" || i.FullName == "CSharpExtensions.Contracts.Abstractions.IUserCommand");
        var isUserEvent = interfaces.Any(i => i.Name == "IUserEvent" || i.FullName == "CSharpExtensions.Contracts.Abstractions.IUserEvent");
        var isEmployeeCommand = interfaces.Any(i => i.Name == "IEmployeeCommand" || i.FullName == "CSharpExtensions.Contracts.Abstractions.IEmployeeCommand");
        var isEmployeeEvent = interfaces.Any(i => i.Name == "IEmployeeEvent" || i.FullName == "CSharpExtensions.Contracts.Abstractions.IEmployeeEvent");
        var isPolicyV5 = isUserCommand || isUserEvent || isEmployeeCommand || isEmployeeEvent;
        var violations = new List<string>();

        if (isPolicyV5)
        {
            var v5Violations = new List<string>();

            // Validate the 5 required metadata fields/properties
            var messageTypeVal = GetMetadataValue(messageType, "MessageType");
            var domainVal = GetMetadataValue(messageType, "Domain");
            var aggregateVal = GetMetadataValue(messageType, "Aggregate");
            var actionVal = GetMetadataValue(messageType, "Action");
            var versionVal = GetMetadataIntValue(messageType, "Version");

            if (string.IsNullOrEmpty(messageTypeVal)) v5Violations.Add("MessageType constant/property is missing or empty.");
            if (string.IsNullOrEmpty(domainVal)) v5Violations.Add("Domain constant/property is missing or empty.");
            if (string.IsNullOrEmpty(aggregateVal)) v5Violations.Add("Aggregate constant/property is missing or empty.");
            if (string.IsNullOrEmpty(actionVal)) v5Violations.Add("Action constant/property is missing or empty.");
            if (versionVal == null) v5Violations.Add("Version constant/property is missing or invalid.");

            // Validate envelope & identity fields
            var props = messageType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            
            var hasMessageId = props.Any(p => p.Name == "MessageId" && p.PropertyType == typeof(Guid));
            var hasOccurredAtUtc = props.Any(p => p.Name == "OccurredAtUtc" && p.PropertyType == typeof(DateTime));
            var hasTenantId = props.Any(p => p.Name == "TenantId" && p.PropertyType == typeof(int));
            var hasPartnerIdProp = props.Any(p => p.Name == "PartnerId" && p.PropertyType == typeof(int));

            if (!hasMessageId) v5Violations.Add("Required 'Guid MessageId' property is missing.");
            if (!hasOccurredAtUtc) v5Violations.Add("Required 'DateTime OccurredAtUtc' property is missing.");
            if (!hasTenantId) v5Violations.Add("Required 'int TenantId' property is missing.");
            if (!hasPartnerIdProp) v5Violations.Add("Required 'int PartnerId' property is missing.");

            if (isUserCommand || isUserEvent)
            {
                var hasUserId = props.Any(p => p.Name == "UserId" && p.PropertyType == typeof(Guid));
                if (!hasUserId) v5Violations.Add("Required 'Guid UserId' property is missing for User contract.");
            }
            else if (isEmployeeCommand || isEmployeeEvent)
            {
                var hasEmployeeId = props.Any(p => p.Name == "EmployeeId" && p.PropertyType == typeof(Guid));
                if (!hasEmployeeId) v5Violations.Add("Required 'Guid EmployeeId' property is missing for Employee contract.");
            }

            if (v5Violations.Count > 0)
            {
                violations.AddRange(v5Violations);
            }
        }

        var properties = messageType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        // 1. Enforce presence of PartnerId (case-insensitive)
        var hasPartnerId = properties.Any(p => string.Equals(p.Name, "PartnerId", StringComparison.OrdinalIgnoreCase));
        if (!hasPartnerId)
        {
            violations.Add(
                $"Message type '{messageType.Name}' must contain a 'PartnerId' property (e.g. public int PartnerId {{ get; set; }}).");
        }

        // 2. Validate class name version suffix (e.g. V1Message or V1)
        var nameMatch = Regex.Match(messageType.Name, @"V(\d+)(Message)?$");
        if (!nameMatch.Success)
        {
            violations.Add(
                $"Message type '{messageType.Name}' name is invalid. It must end with 'V[Version]Message' (e.g. 'OrderLifecycleV1Message') or 'V[Version]' (e.g. 'BillingEventsInvoiceCreatedV1').");
        }
        else
        {
            if (!int.TryParse(nameMatch.Groups[1].Value, out var classNameVersion)
                || classNameVersion <= 0)
            {
                violations.Add(
                    $"Message type '{messageType.Name}' contains an invalid version number in its suffix '{nameMatch.Value}'.");
            }
            else
            {
                var versionProp = properties.FirstOrDefault(p => string.Equals(p.Name, "Version", StringComparison.OrdinalIgnoreCase));
                if (versionProp == null)
                {
                    violations.Add(
                        $"Message type '{messageType.Name}' must contain a read-only integer 'Version' property (e.g. public int Version => {classNameVersion};).");
                }
                else
                {
                    var versionVal = GetMetadataIntValue(messageType, "Version");
                    if (versionVal.HasValue && versionVal.Value != classNameVersion)
                    {
                        violations.Add(
                            $"Message type '{messageType.Name}' version mismatch. The class name implies version {classNameVersion}, but the 'Version' property returns {versionVal.Value}.");
                    }
                }
            }
        }

        // 3. Check obsolete SchemaVersion property
        var schemaVersionProp = properties.FirstOrDefault(p => string.Equals(p.Name, "SchemaVersion", StringComparison.OrdinalIgnoreCase));
        if (schemaVersionProp != null)
        {
            violations.Add(
                $"Message type '{messageType.Name}' contains obsolete 'SchemaVersion' property. Use 'Version' integer property instead.");
        }

        // 4. Validate constant string Topic matching rules
        var topicValue = GetTopicConstantValue(messageType);
        if (topicValue != null)
        {
            if (topicValue.Contains('_'))
            {
                violations.Add(
                    $"Message type '{messageType.Name}' defines Topic '{topicValue}' which contains underscores ('_'). " +
                    "Kafka topic names must use hyphens/dots only.");
            }
        }

        // 5. Validate properties
        foreach (var property in properties)
        {
            var name = property.Name;
            var serializedName = GetSerializedName(property);

            // A. Check for prohibited naming conventions
            if (ProhibitedPropertyRegex.IsMatch(name))
            {
                violations.Add(
                    $"Property '{name}' on message type '{messageType.Name}' uses a prohibited naming convention. " +
                    $"Use 'UserId' (Client/User context) or 'EmployeeId' (Employee/Staff context). " +
                    $"Create a new DTO version and register an IMessageUpcaster for legacy message migration.");
                continue;
            }

            // B. Check for strict camelCase of the serialized field name
            if (!IsCamelCase(serializedName))
            {
                violations.Add(
                    $"Property '{name}' (serialized as '{serializedName}') on message type '{messageType.Name}' violates the camelCase convention. " +
                    $"All fields must be strictly in camelCase (no underscores/hyphens, starts with lowercase).");
            }
        }

        if (violations.Count > 0)
        {
            throw new InvalidOperationException(
                $"Kafka message naming convention violations detected:\n" +
                string.Join("\n", violations.Select((violation, index) => $"  [{index + 1}] {violation}")) +
                "\n\nSee: IMessageUpcaster documentation for migration guidance.");
        }
    }

    private static string GetSerializedName(PropertyInfo property)
    {
        var attribute = property.GetCustomAttribute<JsonPropertyNameAttribute>();
        if (attribute is not null && !string.IsNullOrEmpty(attribute.Name))
        {
            return attribute.Name;
        }

        return JsonNamingPolicy.CamelCase.ConvertName(property.Name);
    }

    private static bool IsCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return false;
        }

        // Must start with lowercase letter
        if (!char.IsLower(name[0]))
        {
            return false;
        }

        // Must not contain underscores or hyphens
        if (name.Contains('_') || name.Contains('-'))
        {
            return false;
        }

        // Must be alphanumeric
        return name.All(char.IsLetterOrDigit);
    }

    private static string? GetTopicConstantValue(Type type)
    {
        var field = type.GetField("Topic", BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
        if (field != null && field.IsLiteral && !field.IsInitOnly && field.FieldType == typeof(string))
        {
            return (string?)field.GetValue(null);
        }

        var prop = type.GetProperty("Topic", BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
        if (prop != null && prop.PropertyType == typeof(string))
        {
            return (string?)prop.GetValue(null);
        }

        return null;
    }

    private static string? GetMetadataValue(Type type, string fieldName)
    {
        var field = type.GetField(fieldName, BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.FlattenHierarchy);
        if (field != null && field.FieldType == typeof(string))
        {
            if (field.IsStatic)
            {
                return (string?)field.GetValue(null);
            }
            try
            {
                var dummy = RuntimeHelpers.GetUninitializedObject(type);
                return (string?)field.GetValue(dummy);
            }
            catch
            {
                return null;
            }
        }

        var prop = type.GetProperty(fieldName, BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.FlattenHierarchy);
        if (prop != null && prop.PropertyType == typeof(string) && prop.CanRead)
        {
            if (prop.GetMethod?.IsStatic == true)
            {
                return (string?)prop.GetValue(null);
            }
            try
            {
                var dummy = RuntimeHelpers.GetUninitializedObject(type);
                return (string?)prop.GetValue(dummy);
            }
            catch
            {
                return null;
            }
        }

        return null;
    }

    private static int? GetMetadataIntValue(Type type, string fieldName)
    {
        var field = type.GetField(fieldName, BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.FlattenHierarchy);
        if (field != null && field.FieldType == typeof(int))
        {
            if (field.IsStatic)
            {
                return (int?)field.GetValue(null);
            }
            try
            {
                var dummy = RuntimeHelpers.GetUninitializedObject(type);
                return (int?)field.GetValue(dummy);
            }
            catch
            {
                return null;
            }
        }

        var prop = type.GetProperty(fieldName, BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.FlattenHierarchy);
        if (prop != null && prop.PropertyType == typeof(int) && prop.CanRead)
        {
            if (prop.GetMethod?.IsStatic == true)
            {
                return (int?)prop.GetValue(null);
            }
            try
            {
                var dummy = RuntimeHelpers.GetUninitializedObject(type);
                return (int?)prop.GetValue(dummy);
            }
            catch
            {
                return null;
            }
        }

        return null;
    }
}
