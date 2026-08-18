namespace CSharpExtensions.AspNetCore.Auth.Constants;

/// <summary>
/// Stable names for authentication-scheme-specific authorization policies.
/// </summary>
public static class AuthorizationPolicies
{
    public const string JwtOnly = "JwtOnly";
    public const string S2SOnly = "S2SOnly";
}
