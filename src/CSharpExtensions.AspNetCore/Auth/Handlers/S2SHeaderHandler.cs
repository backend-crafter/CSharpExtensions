using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using CSharpExtensions.AspNetCore.Auth.Extensions;
using CSharpExtensions.AspNetCore.Auth.Options;
using CSharpExtensions.Foundation.Helpers.Constants;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.AspNetCore.Auth.Handlers;

/// <summary>
/// Adds S2S credentials and delegated actor context after validating the destination policy.
/// </summary>
public sealed class S2SHeaderHandler(
    IOptions<S2SAuthOptions> options,
    IHttpContextAccessor? httpContextAccessor = null) : DelegatingHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        var settings = options.Value;
        ValidateDestination(request.RequestUri, settings);
        ClearManagedCredentials(request);

        var token = settings.Token;
        if (!string.IsNullOrEmpty(token))
        {
            if (token.Length > settings.MaximumHeaderValueLength)
            {
                throw new InvalidOperationException("The configured S2S credential exceeds the header limit.");
            }

            SetHeader(request, CustomRequestHeaders.S2SToken, token);

            if (settings.CredentialHeaderMode == S2SCredentialHeaderMode.Compatibility)
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
                SetHeader(request, CustomRequestHeaders.InternalApiKey, token);
            }
            else
            {
                request.Headers.Authorization = null;
            }
        }

        if (settings.ForwardActorContext && httpContextAccessor?.HttpContext is { } httpContext)
        {
            request.ApplyActorContext(httpContext);
        }

        return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateDestination(Uri? destination, S2SAuthOptions options)
    {
        if (options.DestinationValidation != S2SDestinationValidationMode.Strict)
        {
            return;
        }

        if (destination is null || !destination.IsAbsoluteUri)
        {
            throw new InvalidOperationException("Strict S2S destination validation requires an absolute request URI.");
        }

        if (!string.Equals(destination.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("S2S credentials can only be sent over HTTPS in strict mode.");
        }

        var canonicalDestinationHost = CanonicalizeHost(destination.IdnHost);
        if (options.AllowedHosts is not { Count: > 0 } ||
            !options.AllowedHosts.Any(host => string.Equals(
                CanonicalizeHost(host),
                canonicalDestinationHost,
                StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException("The S2S destination host is not allowed.");
        }
    }

    private static string CanonicalizeHost(string host)
    {
        var candidate = host.Trim().TrimEnd('.');
        if (IPAddress.TryParse(candidate, out var address))
        {
            return address.ToString();
        }

        try
        {
            return new IdnMapping().GetAscii(candidate);
        }
        catch (ArgumentException)
        {
            return string.Empty;
        }
    }

    private static void SetHeader(HttpRequestMessage request, string name, string value)
    {
        request.Headers.Remove(name);
        request.Headers.TryAddWithoutValidation(name, value);
    }

    private static void ClearManagedCredentials(HttpRequestMessage request)
    {
        request.Headers.Remove(CustomRequestHeaders.S2SToken);
        request.Headers.Remove(CustomRequestHeaders.S2S);
        request.Headers.Remove(CustomRequestHeaders.InternalApiKey);
        request.Headers.Authorization = null;
    }
}
