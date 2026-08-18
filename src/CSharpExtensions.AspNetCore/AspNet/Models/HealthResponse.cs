using System.Text.Json.Serialization;

namespace CSharpExtensions.AspNetCore.AspNet.Models;

/// <summary>
/// Represents the root standardized health check response structure.
/// </summary>
public sealed record HealthResponse(
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("version")] HealthVersion Version,
    [property: JsonPropertyName("totalDuration")] string TotalDuration,
    [property: JsonPropertyName("results")] IEnumerable<HealthResult> Results
);
