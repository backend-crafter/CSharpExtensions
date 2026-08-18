using System.Text.Json.Serialization;

namespace CSharpExtensions.AspNetCore.AspNet.Models;

/// <summary>
/// Represents the assembly and Git metadata version details.
/// </summary>
public sealed record HealthVersion(
    [property: JsonPropertyName("commit")] string Commit,
    [property: JsonPropertyName("branch")] string Branch,
    [property: JsonPropertyName("version")] string Version,
    [property: JsonPropertyName("built")] string Built
);
