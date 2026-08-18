namespace CSharpExtensions.AspNetCore.Auth.Options;

/// <summary>
/// Defines a per-registration browser CORS policy.
/// </summary>
public sealed class CorsPolicyOptions
{
    /// <summary>Gets or sets explicitly allowed browser origins.</summary>
    public List<string> AllowedOrigins { get; set; } = [];

    /// <summary>Gets or sets allowed HTTP methods.</summary>
    public List<string> AllowedMethods { get; set; } = [];

    /// <summary>Gets or sets allowed browser request headers.</summary>
    public List<string> AllowedHeaders { get; set; } = [];

    /// <summary>Gets or sets whether browser credentials are allowed.</summary>
    public bool AllowCredentials { get; set; } = true;

    /// <summary>Gets or sets whether loopback HTTP origins are accepted for local development.</summary>
    public bool AllowLoopbackHttp { get; set; }
}
