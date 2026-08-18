using CSharpExtensions.AspNetCore.Auth.Constants;
using CSharpExtensions.AspNetCore.Auth.Handlers;
using CSharpExtensions.AspNetCore.Auth.Options;
using CSharpExtensions.Core.Helpers.Constants;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;

namespace CSharpExtensions.AspNetCore.Auth.Extensions;

/// <summary>
/// Flexible extension methods for registering authentication and authorization.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Configures isolated default browser CORS policies for this service registration.
    /// Standard safe HTTP methods and headers are used as defaults.
    /// </summary>
    public static IServiceCollection AddCorsPolicy(
        this IServiceCollection services,
        Action<List<string>>? configureOrigins = null,
        Action<List<string>>? configureMethods = null,
        Action<List<string>>? configureHeaders = null)
    {
        var registration = new CorsPolicyOptions();

        configureOrigins?.Invoke(registration.AllowedOrigins);
        configureMethods?.Invoke(registration.AllowedMethods);
        configureHeaders?.Invoke(registration.AllowedHeaders);

        return services.AddCorsPolicy(registration);
    }

    /// <summary>
    /// Configures a browser CORS policy from an isolated options instance.
    /// </summary>
    public static IServiceCollection AddCorsPolicy(
        this IServiceCollection services,
        Action<CorsPolicyOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var registration = new CorsPolicyOptions();
        configure(registration);
        return services.AddCorsPolicy(registration);
    }

    /// <summary>
    /// Configures the Default CORS Policy using values from the application configuration.
    /// </summary>
    public static IServiceCollection AddCorsPolicy(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionKey = "Cors")
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var corsSection = configuration.GetSection(sectionKey);
        var registration = corsSection.Get<CorsPolicyOptions>() ?? new CorsPolicyOptions();
        return services.AddCorsPolicy(registration);
    }

    private static readonly string[] DefaultCorsMethods =
    [
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "OPTIONS"
    ];

    private static readonly string[] DefaultCorsHeaders =
    [
        "content-type",
        "authorization",
        "x-requested-with",
        CustomRequestHeaders.ApiVersion,
        CustomRequestHeaders.CorrelationId,
        CustomRequestHeaders.RequestId
    ];

    private static IServiceCollection AddCorsPolicy(
        this IServiceCollection services,
        CorsPolicyOptions registration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(registration);

        if (registration.AllowedOrigins is null ||
            registration.AllowedMethods is null ||
            registration.AllowedHeaders is null)
        {
            throw new InvalidOperationException("CORS origin, method, and header collections cannot be null.");
        }

        var origins = GetCorsOrigins(registration);
        var methods = NormalizeTokens(
            registration.AllowedMethods.Count > 0
                ? registration.AllowedMethods
                : DefaultCorsMethods,
            "CORS method");
        var headers = NormalizeTokens(
            registration.AllowedHeaders.Count > 0
                ? registration.AllowedHeaders
                : DefaultCorsHeaders,
            "CORS header");

        if (headers.Any(IsInternalBrowserHeader))
        {
            throw new InvalidOperationException("Internal credential or messaging headers cannot be exposed through browser CORS.");
        }

        services.AddCors(options =>
        {
            void ConfigurePolicy(Microsoft.AspNetCore.Cors.Infrastructure.CorsPolicyBuilder policy)
            {
                policy.WithOrigins(origins)
                    .WithMethods(methods)
                    .WithHeaders(headers);

                if (registration.AllowCredentials)
                {
                    policy.AllowCredentials();
                }
            }

            options.AddDefaultPolicy(ConfigurePolicy);
            options.AddPolicy("DefaultPolicy", ConfigurePolicy);
        });

        return services;
    }

    private static string[] GetCorsOrigins(CorsPolicyOptions options)
    {
        var candidates = options.AllowedOrigins;

        var origins = new List<string>();
        foreach (var candidate in candidates)
        {
            if (!Uri.TryCreate(candidate, UriKind.Absolute, out var uri) ||
                !string.IsNullOrEmpty(uri.UserInfo) ||
                !string.IsNullOrEmpty(uri.Query) ||
                !string.IsNullOrEmpty(uri.Fragment) ||
                uri.AbsolutePath != "/" ||
                (!string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase) &&
                 !(options.AllowLoopbackHttp && uri.IsLoopback &&
                   string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase))))
            {
                throw new InvalidOperationException(
                    "CORS origins must be authority-only HTTPS URIs, except explicitly enabled loopback HTTP origins.");
            }

            origins.Add(uri.GetLeftPart(UriPartial.Authority));
        }

        return origins.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    private static string[] NormalizeTokens(IEnumerable<string> values, string fieldName)
    {
        var result = new List<string>();
        foreach (var value in values)
        {
            if (string.IsNullOrWhiteSpace(value) || !IsHttpToken(value))
            {
                throw new InvalidOperationException($"{fieldName} values must be non-empty valid HTTP tokens.");
            }

            result.Add(value);
        }

        return result.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    private static bool IsHttpToken(string value)
    {
        foreach (var character in value)
        {
            if (char.IsAsciiLetterOrDigit(character) ||
                character is '!' or '#' or '$' or '%' or '&' or '\'' or '*' or '+' or '-' or '.' or '^' or '_' or '`' or '|' or '~')
            {
                continue;
            }

            return false;
        }

        return true;
    }

    private static bool IsInternalBrowserHeader(string header)
        => header.Equals(CustomRequestHeaders.S2SToken, StringComparison.OrdinalIgnoreCase) ||
           header.Equals(CustomRequestHeaders.S2S, StringComparison.OrdinalIgnoreCase) ||
           header.Equals(CustomRequestHeaders.InternalApiKey, StringComparison.OrdinalIgnoreCase) ||
           header.Equals(CustomRequestHeaders.MessageSignature, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Adds hybrid authentication supporting both standard JWT Bearer and static S2S tokens.
    /// </summary>
    public static IServiceCollection AddHybridAuthentication(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<JwtBearerAuthOptions>? configureJwt = null,
        Action<S2SAuthOptions>? configureS2S = null)
    {
        // 1. Add standard JWT Bearer
        services.AddJwtBearerAuth(configuration, JwtBearerAuthOptions.SectionName, configureJwt);

        // 2. Add S2S for service-to-service calls
        services.AddS2S(configuration, S2SAuthOptions.SchemeName, configureS2S);

        // 3. Configure Default Policy to evaluate both schemes
        services.AddAuthorization(options =>
        {
            options.DefaultPolicy = new AuthorizationPolicyBuilder(
                JwtBearerDefaults.AuthenticationScheme,
                S2SAuthOptions.SchemeName)
                .RequireAuthenticatedUser()
                .Build();

            options.AddPolicy(
                AuthorizationPolicies.JwtOnly,
                policy => policy
                    .AddAuthenticationSchemes(JwtBearerDefaults.AuthenticationScheme)
                    .RequireAuthenticatedUser());

            options.AddPolicy(
                AuthorizationPolicies.S2SOnly,
                policy => policy
                    .AddAuthenticationSchemes(S2SAuthOptions.SchemeName)
                    .RequireAuthenticatedUser());
        });

        return services;
    }

    /// <summary>
    /// Adds standard JWT Bearer authentication.
    /// </summary>
    public static IServiceCollection AddJwtBearerAuth(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionKey = JwtBearerAuthOptions.SectionName,
        Action<JwtBearerAuthOptions>? configure = null)
    {
        var options = new JwtBearerAuthOptions();
        var section = configuration.GetSection(sectionKey);
        if (section.Exists())
        {
            section.Bind(options);
        }

        configure?.Invoke(options);

        ValidateOptions(options, new JwtBearerAuthOptionsValidator(), sectionKey);
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<JwtBearerAuthOptions>, JwtBearerAuthOptionsValidator>());
        services.AddOptions<JwtBearerAuthOptions>()
            .Configure(destination => CopyJwtOptions(options, destination))
            .ValidateOnStart();

        services.AddAuthentication()
            .AddJwtBearer(JwtBearerDefaults.AuthenticationScheme, jwtOptions =>
            {
                jwtOptions.Authority = options.Authority;
                jwtOptions.RequireHttpsMetadata = options.RequireHttpsMetadata;
                jwtOptions.MapInboundClaims = true;

                var tokenValidationParameters = new TokenValidationParameters
                {
                    ValidateIssuer = options.ValidateIssuer,
                    ValidIssuer = options.Authority,
                    ValidateAudience = options.ValidateAudience || !string.IsNullOrWhiteSpace(options.Audience),
                    ValidAudience = options.Audience,
                    ValidateIssuerSigningKey = true,
                    ValidateLifetime = options.ValidateLifetime,
                    ClockSkew = TimeSpan.FromSeconds(options.ClockSkewSeconds)
                };

                if (options.ValidIssuers.Count > 0)
                {
                    tokenValidationParameters.ValidIssuers = options.ValidIssuers;
                }

                if (options.ValidAudiences.Count > 0)
                {
                    tokenValidationParameters.ValidAudiences = options.ValidAudiences;
                }

                jwtOptions.TokenValidationParameters = tokenValidationParameters;
            });

        return services;
    }

    /// <summary>
    /// Adds Service-to-Service (S2S) static token authentication.
    /// Priority: manual configuration (Action) > Configuration Section.
    /// </summary>
    public static IServiceCollection AddS2S(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionKey = S2SAuthOptions.SchemeName,
        Action<S2SAuthOptions>? configure = null)
    {
        var options = new S2SAuthOptions();

        var section = configuration.GetSection(sectionKey);
        if (section.Exists())
        {
            section.Bind(options);
        }

        configure?.Invoke(options);

        ValidateOptions(options, new S2SAuthOptionsValidator(), sectionKey);
        RegisterS2SClientServices(services, options);

        services.AddAuthentication()
            .AddScheme<S2SAuthOptions, S2SAuthenticationHandler>(S2SAuthOptions.SchemeName, opt =>
            {
                CopyS2SOptions(options, opt);
            });

        services.AddAuthentication()
            .AddScheme<S2SAuthOptions, S2SAuthenticationHandler>("InternalServiceToken", opt =>
            {
                CopyS2SOptions(options, opt);
            });

        return services;
    }

    /// <summary>
    /// Registers S2S client-side dependencies (S2SHeaderHandler and S2SAuthOptions)
    /// without configuring full inbound ASP.NET Core authentication.
    /// Ideal for background workers and services that only make outgoing S2S requests.
    /// </summary>
    public static IServiceCollection AddS2SOnly(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        var options = new S2SAuthOptions();
        var section = configuration.GetSection(S2SAuthOptions.SchemeName);
        if (section.Exists())
        {
            section.Bind(options);
        }

        ValidateOptions(options, new S2SAuthOptionsValidator(), S2SAuthOptions.SchemeName);
        RegisterS2SClientServices(services, options);
        return services;
    }

    /// <summary>
    /// Adds the S2S authentication header handler to the HTTP client.
    /// Credentials are applied according to <see cref="S2SCredentialHeaderMode"/>, and automatic redirects are disabled
    /// so custom credentials cannot be replayed to a redirected host.
    /// </summary>
    public static IHttpClientBuilder AddS2SAuth(this IHttpClientBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.Configure<S2SHttpClientSecurityOptions>(
            builder.Name,
            options => options.Enabled = true);
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHttpMessageHandlerBuilderFilter, S2SRedirectGuardFilter>());

        return builder.AddHttpMessageHandler<S2SHeaderHandler>();
    }

    private static void RegisterS2SClientServices(IServiceCollection services, S2SAuthOptions options)
    {
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<S2SAuthOptions>, S2SAuthOptionsValidator>());
        services.AddOptions<S2SAuthOptions>()
            .Configure(destination => CopyS2SOptions(options, destination))
            .ValidateOnStart();
        services.AddHttpContextAccessor();
        services.TryAddTransient<S2SHeaderHandler>();
    }

    private static void CopyS2SOptions(S2SAuthOptions source, S2SAuthOptions destination)
    {
        destination.Token = source.Token;
        destination.DestinationValidation = source.DestinationValidation;
        destination.AllowedHosts = source.AllowedHosts is null ? [] : [.. source.AllowedHosts];
        destination.CredentialHeaderMode = source.CredentialHeaderMode;
        destination.MaximumHeaderValueLength = source.MaximumHeaderValueLength;
        destination.ForwardActorContext = source.ForwardActorContext;
    }

    private static void CopyJwtOptions(JwtBearerAuthOptions source, JwtBearerAuthOptions destination)
    {
        destination.Authority = source.Authority;
        destination.Audience = source.Audience;
        destination.RequireHttpsMetadata = source.RequireHttpsMetadata;
        destination.ValidateIssuer = source.ValidateIssuer;
        destination.ValidateAudience = source.ValidateAudience;
        destination.ValidateLifetime = source.ValidateLifetime;
        destination.ClockSkewSeconds = source.ClockSkewSeconds;
        destination.ValidIssuers = [.. source.ValidIssuers];
        destination.ValidAudiences = [.. source.ValidAudiences];
    }

    private static void ValidateOptions<TOptions>(
        TOptions options,
        IValidateOptions<TOptions> validator,
        string configurationSection)
        where TOptions : class
    {
        var result = validator.Validate(Microsoft.Extensions.Options.Options.DefaultName, options);
        if (result.Failed)
        {
            throw new OptionsValidationException(
                configurationSection,
                typeof(TOptions),
                result.Failures);
        }
    }
}
