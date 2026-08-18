namespace CSharpExtensions.AspNetCore.Auth.Options;

/// <summary>
/// Controls validation of destinations that receive internal S2S credentials.
/// </summary>
public enum S2SDestinationValidationMode
{
    /// <summary>Preserves existing destinations during the migration window.</summary>
    Compatibility = 0,

    /// <summary>Requires HTTPS and an explicitly allowed destination host.</summary>
    Strict = 1
}
