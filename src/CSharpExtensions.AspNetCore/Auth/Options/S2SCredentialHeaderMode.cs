namespace CSharpExtensions.AspNetCore.Auth.Options;

/// <summary>
/// Selects which credential headers are emitted by the S2S HTTP handler.
/// </summary>
public enum S2SCredentialHeaderMode
{
    /// <summary>Emits current and legacy headers for compatibility.</summary>
    Compatibility = 0,

    /// <summary>Emits only the canonical X-S2S-Token header.</summary>
    Canonical = 1
}
