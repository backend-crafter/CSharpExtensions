using System.Text.Json;

namespace CSharpExtensions.Core.Json.Policies;

/// <summary>
/// A JSON naming policy that converts names to lowercase.
/// </summary>
public sealed class LowerCaseNamingPolicy : JsonNamingPolicy
{
    /// <summary>
    /// Static instance of the policy.
    /// </summary>
    public static LowerCaseNamingPolicy Instance { get; } = new();

    /// <inheritdoc />
    public override string ConvertName(string name)
    {
        return string.IsNullOrWhiteSpace(name) ? name : name.ToLowerInvariant();
    }
}
