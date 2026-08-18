namespace CSharpExtensions.Kafka.Core;

using System;
using System.Text.RegularExpressions;

/// <summary>
/// Validates SQL Server identifiers to prevent SQL injection in DDL operations.
/// </summary>
internal static partial class SqlIdentifierValidator
{
    private static readonly Regex ValidIdentifierPattern = new(
        @"^[a-zA-Z_][a-zA-Z0-9_]{0,127}$",
        RegexOptions.Compiled);

    /// <summary>
    /// Validates that a string is a safe SQL Server identifier.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the identifier contains invalid characters.</exception>
    public static string ValidateIdentifier(string identifier, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(identifier, parameterName);

        if (!ValidIdentifierPattern.IsMatch(identifier))
        {
            throw new ArgumentException(
                $"'{identifier}' is not a valid SQL Server identifier. " +
                "Only letters, digits, and underscores are allowed, starting with a letter or underscore.",
                parameterName);
        }

        return identifier;
    }
}
