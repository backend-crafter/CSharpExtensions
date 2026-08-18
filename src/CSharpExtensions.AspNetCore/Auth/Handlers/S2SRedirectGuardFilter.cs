using Microsoft.Extensions.Http;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.AspNetCore.Auth.Handlers;

internal sealed class S2SHttpClientSecurityOptions
{
    internal bool Enabled { get; set; }
}

/// <summary>
/// Validates the final primary-handler pipeline after all named-client configuration has run.
/// </summary>
internal sealed class S2SRedirectGuardFilter(
    IOptionsMonitor<S2SHttpClientSecurityOptions> options) : IHttpMessageHandlerBuilderFilter
{
    public Action<HttpMessageHandlerBuilder> Configure(Action<HttpMessageHandlerBuilder> next)
    {
        ArgumentNullException.ThrowIfNull(next);

        return builder =>
        {
            next(builder);
            if (!options.Get(builder.Name).Enabled)
            {
                return;
            }

            switch (builder.PrimaryHandler)
            {
                case HttpClientHandler httpClientHandler:
                    httpClientHandler.AllowAutoRedirect = false;
                    break;
                case SocketsHttpHandler socketsHttpHandler:
                    socketsHttpHandler.AllowAutoRedirect = false;
                    break;
                default:
                    throw new InvalidOperationException(
                        "S2S authentication requires a primary HTTP handler whose automatic redirects can be disabled.");
            }
        };
    }
}
