using Microsoft.Extensions.Options;

namespace CSharpExtensions.AspNetCore.Auth.Options;

/// <summary>
/// Validates <see cref="JwtBearerAuthOptions"/> configuration.
/// </summary>
public sealed class JwtBearerAuthOptionsValidator : IValidateOptions<JwtBearerAuthOptions>
{
    public ValidateOptionsResult Validate(string? name, JwtBearerAuthOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.Authority))
        {
            return ValidateOptionsResult.Fail("Jwt:Authority is required.");
        }

        if (!Uri.TryCreate(options.Authority, UriKind.Absolute, out var uri))
        {
            return ValidateOptionsResult.Fail("Jwt:Authority must be a valid absolute URI.");
        }

        if (options.RequireHttpsMetadata && !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            return ValidateOptionsResult.Fail("Jwt:Authority must use HTTPS when RequireHttpsMetadata is true.");
        }

        if (options.ClockSkewSeconds < 0 || options.ClockSkewSeconds > 600)
        {
            return ValidateOptionsResult.Fail("Jwt:ClockSkewSeconds must be between 0 and 600 seconds.");
        }

        return ValidateOptionsResult.Success;
    }
}
