using Microsoft.Extensions.Options;

namespace CSharpExtensions.AspNetCore.Auth.Options;

/// <summary>
/// Validates inbound and outbound S2S authentication options.
/// </summary>
public sealed class S2SAuthOptionsValidator : IValidateOptions<S2SAuthOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, S2SAuthOptions options)
    {
        var failures = new List<string>();

        if (string.IsNullOrWhiteSpace(options.Token))
        {
            failures.Add("S2SAuthOptions.Token is required.");
        }
        else if (options.Token.Any(char.IsControl))
        {
            failures.Add("S2SAuthOptions.Token cannot contain control characters.");
        }
        else if (options.Token.Length > options.MaximumHeaderValueLength)
        {
            failures.Add("S2SAuthOptions.Token exceeds MaximumHeaderValueLength.");
        }

        if (options.MaximumHeaderValueLength is < 32 or > 16 * 1024)
        {
            failures.Add("S2SAuthOptions.MaximumHeaderValueLength must be between 32 and 16384.");
        }

        if (!Enum.IsDefined(options.DestinationValidation))
        {
            failures.Add("S2SAuthOptions.DestinationValidation is invalid.");
        }

        if (!Enum.IsDefined(options.CredentialHeaderMode))
        {
            failures.Add("S2SAuthOptions.CredentialHeaderMode is invalid.");
        }

        if (options.AllowedHosts is null)
        {
            failures.Add("S2SAuthOptions.AllowedHosts cannot be null.");
        }
        else
        {
            foreach (var host in options.AllowedHosts)
            {
                if (!IsValidHost(host))
                {
                    failures.Add("S2SAuthOptions.AllowedHosts contains an invalid host.");
                    break;
                }
            }
        }

        if (options.DestinationValidation == S2SDestinationValidationMode.Strict &&
            options.AllowedHosts is not { Count: > 0 })
        {
            failures.Add("Strict S2S destination validation requires at least one allowed host.");
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }

    private static bool IsValidHost(string? host)
    {
        if (string.IsNullOrWhiteSpace(host) || host.Length > 253 ||
            host.Contains('/') || host.Contains('@') || host.Any(char.IsWhiteSpace))
        {
            return false;
        }

        return Uri.CheckHostName(host) is UriHostNameType.Dns or UriHostNameType.IPv4 or UriHostNameType.IPv6;
    }
}
