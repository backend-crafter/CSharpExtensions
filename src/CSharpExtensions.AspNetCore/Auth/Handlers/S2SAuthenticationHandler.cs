using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using CSharpExtensions.AspNetCore.Auth.Options;
using CSharpExtensions.Foundation.Helpers.Constants;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;

namespace CSharpExtensions.AspNetCore.Auth.Handlers;

/// <summary>
/// Authenticates bounded S2S credential headers using constant-time comparison.
/// </summary>
public sealed class S2SAuthenticationHandler(
    IOptionsMonitor<S2SAuthOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder)
    : AuthenticationHandler<S2SAuthOptions>(options, logger, encoder)
{
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var readResult = ReadProvidedToken();
        if (readResult.IsInvalid)
        {
            return Task.FromResult(AuthenticateResult.Fail("The S2S credential header is invalid."));
        }

        var providedToken = readResult.Value;
        if (string.IsNullOrWhiteSpace(providedToken))
        {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        if (IsJwtToken(providedToken))
        {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        var configuredToken = Options.Token;
        if (string.IsNullOrWhiteSpace(configuredToken) ||
            configuredToken.Length > Options.MaximumHeaderValueLength)
        {
            Logger.LogWarning("S2S authentication is unavailable because its credential configuration is invalid.");
            return Task.FromResult(AuthenticateResult.Fail("S2S authentication is not configured."));
        }

        if (!IsTokenValid(configuredToken, providedToken))
        {
            return Task.FromResult(AuthenticateResult.Fail("Invalid S2S credential."));
        }

        var claims = new[]
        {
            new Claim(ClaimTypes.Name, "S2S"),
            new Claim("scope", "internal"),
            new Claim("identity_type", "service")
        };
        var identity = new ClaimsIdentity(claims, Scheme.Name);
        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, Scheme.Name);

        return Task.FromResult(AuthenticateResult.Success(ticket));
    }

    private TokenReadResult ReadProvidedToken()
    {
        if (Request.Headers.TryGetValue(CustomRequestHeaders.S2SToken, out var canonical))
        {
            return ReadSingleValue(canonical);
        }

        if (Request.Headers.TryGetValue(CustomRequestHeaders.S2S, out var shortCanonical))
        {
            return ReadSingleValue(shortCanonical);
        }

        if (Request.Headers.TryGetValue("Authorization", out var authorization))
        {
            var value = ReadSingleValue(authorization);
            if (value.IsInvalid || value.Value is null)
            {
                return value;
            }

            const string bearerPrefix = "Bearer ";
            if (!value.Value.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase))
            {
                return TokenReadResult.Invalid;
            }

            var token = value.Value[bearerPrefix.Length..].Trim();
            return IsBounded(token) ? new TokenReadResult(token, false) : TokenReadResult.Invalid;
        }

        if (Request.Headers.TryGetValue(CustomRequestHeaders.InternalApiKey, out var legacy))
        {
            return ReadSingleValue(legacy);
        }

        return default;
    }

    private TokenReadResult ReadSingleValue(StringValues values)
    {
        if (values.Count != 1)
        {
            return TokenReadResult.Invalid;
        }

        var value = values[0];
        return IsBounded(value) ? new TokenReadResult(value, false) : TokenReadResult.Invalid;
    }

    private bool IsBounded(string? value)
        => !string.IsNullOrWhiteSpace(value) &&
           value.Length <= Options.MaximumHeaderValueLength &&
           !value.Any(char.IsControl);

    private static bool IsJwtToken(string token)
    {
        var firstSeparator = token.IndexOf('.');
        if (firstSeparator <= 0)
        {
            return false;
        }

        var secondSeparator = token.IndexOf('.', firstSeparator + 1);
        return secondSeparator > firstSeparator + 1 &&
               secondSeparator < token.Length - 1 &&
               token.IndexOf('.', secondSeparator + 1) < 0;
    }

    private static bool IsTokenValid(string configuredToken, string providedToken)
    {
        var configuredBytes = Encoding.UTF8.GetBytes(configuredToken);
        var providedBytes = Encoding.UTF8.GetBytes(providedToken);
        Span<byte> configuredHash = stackalloc byte[32];
        Span<byte> providedHash = stackalloc byte[32];

        try
        {
            SHA256.HashData(configuredBytes, configuredHash);
            SHA256.HashData(providedBytes, providedHash);
            return CryptographicOperations.FixedTimeEquals(configuredHash, providedHash);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(configuredBytes);
            CryptographicOperations.ZeroMemory(providedBytes);
            CryptographicOperations.ZeroMemory(configuredHash);
            CryptographicOperations.ZeroMemory(providedHash);
        }
    }

    private readonly record struct TokenReadResult(string? Value, bool IsInvalid)
    {
        internal static TokenReadResult Invalid => new(null, true);
    }
}
