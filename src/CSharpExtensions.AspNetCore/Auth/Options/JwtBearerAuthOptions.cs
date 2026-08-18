namespace CSharpExtensions.AspNetCore.Auth.Options;

/// <summary>
/// Options for configuring standard JWT Bearer authentication.
/// </summary>
public sealed class JwtBearerAuthOptions
{
    public const string SchemeName = "Bearer";
    public const string SectionName = "Jwt";

    /// <summary>
    /// Gets or sets the Authority (OIDC Identity Provider URL or Token Issuer).
    /// </summary>
    public string Authority { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the expected Audience ('aud' claim).
    /// </summary>
    public string? Audience { get; set; }

    /// <summary>
    /// Gets or sets whether HTTPS is required for the Authority metadata. Default is true.
    /// </summary>
    public bool RequireHttpsMetadata { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to validate the token issuer. Default is true.
    /// </summary>
    public bool ValidateIssuer { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to validate the token audience. Default is true when Audience is configured.
    /// </summary>
    public bool ValidateAudience { get; set; }

    /// <summary>
    /// Gets or sets whether to validate token expiration. Default is true.
    /// </summary>
    public bool ValidateLifetime { get; set; } = true;

    /// <summary>
    /// Gets or sets allowed clock skew in seconds. Default is 120 (2 minutes).
    /// </summary>
    public int ClockSkewSeconds { get; set; } = 120;

    /// <summary>
    /// Gets or sets additional valid issuers.
    /// </summary>
    public List<string> ValidIssuers { get; set; } = [];

    /// <summary>
    /// Gets or sets additional valid audiences.
    /// </summary>
    public List<string> ValidAudiences { get; set; } = [];
}
