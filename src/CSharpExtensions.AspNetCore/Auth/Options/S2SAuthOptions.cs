using Microsoft.AspNetCore.Authentication;

namespace CSharpExtensions.AspNetCore.Auth.Options;

/// <summary>
/// Options for Service-to-Service (S2S) static token authentication.
/// </summary>
public sealed class S2SAuthOptions : AuthenticationSchemeOptions
{
    /// <summary>
    /// The authentication scheme name.
    /// </summary>
    public const string SchemeName = "S2S";
    
    /// <summary>
    /// The static bearer token used for authentication.
    /// Must be configured via configuration (e.g. "S2S:Token" or secrets).
    /// </summary>
    public string Token { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets destination validation behavior for outbound credentials.
    /// </summary>
    public S2SDestinationValidationMode DestinationValidation { get; set; }

    /// <summary>
    /// Gets or sets the allowed destination hosts used in strict mode.
    /// </summary>
    public List<string> AllowedHosts { get; set; } = [];

    /// <summary>
    /// Gets or sets which S2S credential headers are emitted.
    /// </summary>
    public S2SCredentialHeaderMode CredentialHeaderMode { get; set; }

    /// <summary>
    /// Gets or sets the maximum accepted token/header value length.
    /// </summary>
    public int MaximumHeaderValueLength { get; set; } = 4096;

    /// <summary>
    /// Gets or sets whether actor context is forwarded after destination validation.
    /// </summary>
    public bool ForwardActorContext { get; set; } = true;
}
