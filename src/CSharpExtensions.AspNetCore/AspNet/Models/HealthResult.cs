using System.Text.Json.Serialization;

namespace CSharpExtensions.AspNetCore.AspNet.Models;

/// <summary>
/// Represents the health status of a single system dependency.
/// </summary>
public sealed record HealthResult(
    [property: JsonPropertyName("check")] string Check,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("description")] string Description
);
