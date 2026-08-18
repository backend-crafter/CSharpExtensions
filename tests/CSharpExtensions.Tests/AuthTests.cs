using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using Moq;
using System.Text.Encodings.Web;
using CSharpExtensions.AspNetCore.Auth.Extensions;
using CSharpExtensions.AspNetCore.Auth.Handlers;
using CSharpExtensions.AspNetCore.Auth.Models;
using CSharpExtensions.AspNetCore.Auth.Options;
using CSharpExtensions.Foundation.Helpers.Constants;
using Xunit;

namespace CSharpExtensions.Tests;

public class AuthTests
{
    [Fact]
    public async Task S2SAuthenticationHandler_ValidToken_ShouldSucceed()
    {
        // Arrange
        var options = new S2SAuthOptions { Token = "SecretToken123" };
        var optionsMock = new Mock<IOptionsMonitor<S2SAuthOptions>>();
        optionsMock.Setup(x => x.Get(It.IsAny<string>())).Returns(options);

        var loggerFactoryMock = new Mock<ILoggerFactory>();
        loggerFactoryMock.Setup(x => x.CreateLogger(It.IsAny<string>())).Returns(new Mock<ILogger>().Object);

        var encoderMock = new Mock<UrlEncoder>();

        var handler = new S2SAuthenticationHandler(optionsMock.Object, loggerFactoryMock.Object, encoderMock.Object);

        var context = new DefaultHttpContext();
        context.Request.Headers["Authorization"] = "Bearer SecretToken123";

        await handler.InitializeAsync(new AuthenticationScheme("S2S", "S2S", typeof(S2SAuthenticationHandler)), context);

        // Act
        var result = await handler.AuthenticateAsync();

        // Assert
        Assert.True(result.Succeeded);
        Assert.Equal("S2S", result.Principal?.Identity?.Name);
    }

    [Fact]
    public async Task S2SAuthenticationHandler_InvalidToken_ShouldFail()
    {
        // Arrange
        var options = new S2SAuthOptions { Token = "SecretToken123" };
        var optionsMock = new Mock<IOptionsMonitor<S2SAuthOptions>>();
        optionsMock.Setup(x => x.Get(It.IsAny<string>())).Returns(options);

        var loggerFactoryMock = new Mock<ILoggerFactory>();
        loggerFactoryMock.Setup(x => x.CreateLogger(It.IsAny<string>())).Returns(new Mock<ILogger>().Object);

        var encoderMock = new Mock<UrlEncoder>();

        var handler = new S2SAuthenticationHandler(optionsMock.Object, loggerFactoryMock.Object, encoderMock.Object);

        var context = new DefaultHttpContext();
        context.Request.Headers["Authorization"] = "Bearer WrongToken";

        await handler.InitializeAsync(new AuthenticationScheme("S2S", "S2S", typeof(S2SAuthenticationHandler)), context);

        // Act
        var result = await handler.AuthenticateAsync();

        // Assert
        Assert.False(result.Succeeded);
    }

    [Fact]
    public async Task S2SAuthenticationHandler_JwtToken_ShouldReturnNoResult()
    {
        // Arrange
        var options = new S2SAuthOptions { Token = "SecretToken123" };
        var optionsMock = new Mock<IOptionsMonitor<S2SAuthOptions>>();
        optionsMock.Setup(x => x.Get(It.IsAny<string>())).Returns(options);

        var loggerFactoryMock = new Mock<ILoggerFactory>();
        loggerFactoryMock.Setup(x => x.CreateLogger(It.IsAny<string>())).Returns(new Mock<ILogger>().Object);

        var encoderMock = new Mock<UrlEncoder>();

        var handler = new S2SAuthenticationHandler(optionsMock.Object, loggerFactoryMock.Object, encoderMock.Object);

        var context = new DefaultHttpContext();
        context.Request.Headers["Authorization"] = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c";

        await handler.InitializeAsync(new AuthenticationScheme("S2S", "S2S", typeof(S2SAuthenticationHandler)), context);

        // Act
        var result = await handler.AuthenticateAsync();

        // Assert
        Assert.False(result.Succeeded);
        Assert.True(result.None);
    }

    [Fact]
    public async Task S2SAuthenticationHandler_MultipleCredentialValues_ShouldFailClosed()
    {
        var options = new S2SAuthOptions { Token = "SecretToken123" };
        var optionsMock = new Mock<IOptionsMonitor<S2SAuthOptions>>();
        optionsMock.Setup(instance => instance.Get(It.IsAny<string>())).Returns(options);
        var loggerFactoryMock = new Mock<ILoggerFactory>();
        loggerFactoryMock.Setup(instance => instance.CreateLogger(It.IsAny<string>()))
            .Returns(new Mock<ILogger>().Object);
        var handler = new S2SAuthenticationHandler(
            optionsMock.Object,
            loggerFactoryMock.Object,
            new Mock<UrlEncoder>().Object);
        var context = new DefaultHttpContext();
        context.Request.Headers[CustomRequestHeaders.S2SToken] = new StringValues(["SecretToken123", "SecretToken123"]);
        await handler.InitializeAsync(
            new AuthenticationScheme(S2SAuthOptions.SchemeName, S2SAuthOptions.SchemeName, typeof(S2SAuthenticationHandler)),
            context);

        var result = await handler.AuthenticateAsync();

        Assert.False(result.Succeeded);
        Assert.False(result.None);
    }

    [Fact]
    public void ActorContext_PropertiesAndFactories_WorkCorrectly()
    {
        var userId = Guid.NewGuid();
        var userContext = ActorContext.ForUser(userId, role: "VIP");

        Assert.Equal(userId, userContext.ActorId);
        Assert.Equal(ActorType.User, userContext.ActorType);
        Assert.True(userContext.IsUser);
        Assert.False(userContext.IsEmployee);
        Assert.False(userContext.IsService);
        Assert.Equal("VIP", userContext.Role);

        var employeeId = Guid.NewGuid();
        var employeeContext = ActorContext.ForEmployee(employeeId, role: "Admin");
        Assert.True(employeeContext.IsEmployee);
        Assert.False(employeeContext.IsUser);
        Assert.Equal(ActorType.Employee, employeeContext.ActorType);

        var serviceContext = ActorContext.ForService("S2SGateway");
        Assert.True(serviceContext.IsService);
        Assert.Equal(ActorType.Service, serviceContext.ActorType);
        Assert.Equal("S2S:S2SGateway", serviceContext.ToAuditString());
    }

    [Fact]
    public void ActorContextExtensions_GetActorContext_ParsesClaimsPrincipal()
    {
        var userId = Guid.NewGuid();
        var claims = new[]
        {
            new Claim("sub", userId.ToString()),
            new Claim("role", "ClientUser")
        };
        var identity = new ClaimsIdentity(claims, "TestAuth");
        var principal = new ClaimsPrincipal(identity);

        var actorContext = principal.GetActorContext();

        Assert.Equal(userId, actorContext.ActorId);
        Assert.Equal(ActorType.User, actorContext.ActorType);
        Assert.True(actorContext.IsUser);
        Assert.Equal("ClientUser", actorContext.Role);
    }

    [Fact]
    public void ActorContextExtensions_JwtClaimsCannotImpersonateService()
    {
        var userId = Guid.NewGuid();
        var claims = new[]
        {
            new Claim(ClaimTypes.Name, "S2S"),
            new Claim("scope", "internal"),
            new Claim("identity_type", "service"),
            new Claim("sub", userId.ToString())
        };
        var principal = new ClaimsPrincipal(new ClaimsIdentity(claims, JwtBearerDefaults.AuthenticationScheme));

        var actor = principal.ResolveActorContext();

        Assert.True(actor.IsUser);
        Assert.Equal(userId, actor.ActorId);
    }

    [Fact]
    public void ActorContextExtensions_AnonymousDelegationHeaders_ShouldBeIgnored()
    {
        var context = new DefaultHttpContext();
        context.Request.Headers[CustomRequestHeaders.UserId] = Guid.NewGuid().ToString();
        context.Request.Headers[CustomRequestHeaders.UserEmail] = "spoofed@example.com";

        var actor = context.ResolveActorContext();

        Assert.True(actor.IsAnonymous);
    }

    [Fact]
    public void ActorContextExtensions_TrustedS2SPrincipal_ShouldAllowBoundedDelegation()
    {
        var employeeId = Guid.NewGuid();
        var context = new DefaultHttpContext
        {
            User = CreateServicePrincipal()
        };
        context.Request.Headers[CustomRequestHeaders.ActorType] = ActorType.Employee.ToString();
        context.Request.Headers[CustomRequestHeaders.EmployeeId] = employeeId.ToString();
        context.Request.Headers[CustomRequestHeaders.UserEmail] = "operator@example.com";

        var actor = context.ResolveActorContext();

        Assert.True(actor.IsEmployee);
        Assert.Equal(employeeId, actor.ActorId);
    }

    [Fact]
    public void ActorContext_ToAuditString_ShouldNeverExposeEmailOrDisplayName()
    {
        var userId = Guid.NewGuid();
        var employeeId = Guid.NewGuid();
        var user = ActorContext.ForUser(userId, "user@example.com", "Client Name");
        var employee = ActorContext.ForEmployee(employeeId, "employee@example.com", "Operator Name");

        Assert.Equal($"User:{userId:D}", user.ToAuditString());
        Assert.Equal($"Employee:{employeeId:D}", employee.ToAuditString());
        Assert.DoesNotContain("example.com", user.ToAuditString(), StringComparison.Ordinal);
        Assert.DoesNotContain("Client Name", user.ToAuditString(), StringComparison.Ordinal);
        Assert.DoesNotContain("Operator Name", employee.ToAuditString(), StringComparison.Ordinal);
        Assert.Equal(user.ToAuditString(), user.ToString());

        var userContext = new UserContext("client@example.com", userId, "client@example.com");
        Assert.Equal($"User:{userId:D}", userContext.ToString());
        Assert.DoesNotContain("client@example.com", userContext.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ActorContextExtensions_UnauthenticatedIdentityClaims_ShouldNotOverrideAuthenticatedIdentity()
    {
        var trustedUserId = Guid.NewGuid();
        var spoofedIdentity = new ClaimsIdentity([new Claim("sub", Guid.NewGuid().ToString())]);
        var authenticatedIdentity = new ClaimsIdentity(
            [new Claim("sub", trustedUserId.ToString())],
            JwtBearerDefaults.AuthenticationScheme);
        var principal = new ClaimsPrincipal([spoofedIdentity, authenticatedIdentity]);

        var actor = principal.ResolveActorContext();

        Assert.Equal(trustedUserId, actor.ActorId);
        Assert.True(actor.IsUser);
    }

    [Fact]
    public void ActorContextExtensions_MixedAuthenticatedServiceAndUserIdentities_ShouldFailClosed()
    {
        var userIdentity = new ClaimsIdentity(
            [new Claim("sub", Guid.NewGuid().ToString())],
            JwtBearerDefaults.AuthenticationScheme);
        var context = new DefaultHttpContext
        {
            User = new ClaimsPrincipal([CreateServicePrincipal().Identities.Single(), userIdentity])
        };
        context.Request.Headers[CustomRequestHeaders.UserId] = Guid.NewGuid().ToString();

        var actor = context.ResolveActorContext();

        Assert.True(actor.IsAnonymous);
    }

    [Fact]
    public void ActorContextExtensions_ApplyActorContext_SetsHeaders()
    {
        var userId = Guid.NewGuid();
        var actorContext = ActorContext.ForUser(userId, role: "GoldUser");
        var request = new HttpRequestMessage(HttpMethod.Get, "https://api.internal/test");

        request.ApplyActorContext(actorContext);

        Assert.True(request.Headers.Contains(CustomRequestHeaders.ActorType));
        Assert.Equal("User", request.Headers.GetValues(CustomRequestHeaders.ActorType).First());
        Assert.Equal(userId.ToString(), request.Headers.GetValues(CustomRequestHeaders.ActorId).First());
        Assert.Equal(userId.ToString(), request.Headers.GetValues(CustomRequestHeaders.UserId).First());
        Assert.Equal("GoldUser", request.Headers.GetValues(CustomRequestHeaders.ActorRole).First());

        var employeeId = Guid.NewGuid();
        var employeeContext = ActorContext.ForEmployee(employeeId, role: "Support");
        var empRequest = new HttpRequestMessage(HttpMethod.Get, "https://api.internal/test");

        empRequest.ApplyActorContext(employeeContext);

        Assert.True(empRequest.Headers.Contains(CustomRequestHeaders.ActorType));
        Assert.Equal("Employee", empRequest.Headers.GetValues(CustomRequestHeaders.ActorType).First());
        Assert.Equal(employeeId.ToString(), empRequest.Headers.GetValues(CustomRequestHeaders.ActorId).First());
        Assert.Equal(employeeId.ToString(), empRequest.Headers.GetValues(CustomRequestHeaders.EmployeeId).First());
        Assert.Equal("Support", empRequest.Headers.GetValues(CustomRequestHeaders.ActorRole).First());
    }

    [Fact]
    public void ActorContextExtensions_ApplyActorContext_ShouldReplaceReservedHeadersAndDropInvalidTraceHeaders()
    {
        var userId = Guid.NewGuid();
        var context = new DefaultHttpContext
        {
            User = new ClaimsPrincipal(new ClaimsIdentity(
                [new Claim("sub", userId.ToString())],
                JwtBearerDefaults.AuthenticationScheme))
        };
        context.Request.Headers[CustomRequestHeaders.CorrelationId] = new StringValues(["one", "two"]);
        var request = new HttpRequestMessage(HttpMethod.Get, "https://api.internal/test");
        request.Headers.TryAddWithoutValidation(CustomRequestHeaders.UserId, Guid.NewGuid().ToString());
        request.Headers.TryAddWithoutValidation(CustomRequestHeaders.CorrelationId, "stale-correlation");
        request.Headers.TryAddWithoutValidation(CustomRequestHeaders.RequestId, "stale-request");

        request.ApplyActorContext(context);

        Assert.Equal(userId.ToString(), request.Headers.GetValues(CustomRequestHeaders.UserId).Single());
        Assert.False(request.Headers.Contains(CustomRequestHeaders.CorrelationId));
        Assert.False(request.Headers.Contains(CustomRequestHeaders.RequestId));
    }

    [Fact]
    public async Task S2SHeaderHandler_ForwardsUserContextHeaders()
    {
        var userId = Guid.NewGuid();
        var httpContext = new DefaultHttpContext();
        var claims = new[]
        {
            new Claim("sub", userId.ToString()),
            new Claim("role", "Client")
        };
        httpContext.User = new ClaimsPrincipal(new ClaimsIdentity(claims, "TestAuth"));

        var accessorMock = new Mock<IHttpContextAccessor>();
        accessorMock.Setup(x => x.HttpContext).Returns(httpContext);

        var options = Options.Create(new S2SAuthOptions { Token = "TestS2SToken" });
        var handler = new S2SHeaderHandler(options, accessorMock.Object)
        {
            InnerHandler = new TestHttpMessageHandler()
        };

        var invoker = new HttpMessageInvoker(handler);
        var request = new HttpRequestMessage(HttpMethod.Get, "http://localhost/test");

        var response = await invoker.SendAsync(request, CancellationToken.None);

        Assert.NotNull(response);
        Assert.True(request.Headers.Contains(CustomRequestHeaders.S2SToken));
        Assert.Equal("TestS2SToken", request.Headers.GetValues(CustomRequestHeaders.S2SToken).First());
        Assert.True(request.Headers.Contains(CustomRequestHeaders.ActorType));
        Assert.Equal("User", request.Headers.GetValues(CustomRequestHeaders.ActorType).First());
        Assert.Equal(userId.ToString(), request.Headers.GetValues(CustomRequestHeaders.ActorId).First());
    }

    [Fact]
    public async Task S2SHeaderHandler_StrictCanonicalMode_ShouldEnforceHttpsAllowlistAndRemoveLegacyCredentials()
    {
        var options = Options.Create(new S2SAuthOptions
        {
            Token = "TestS2SToken",
            DestinationValidation = S2SDestinationValidationMode.Strict,
            AllowedHosts = ["api.internal", "::1"],
            CredentialHeaderMode = S2SCredentialHeaderMode.Canonical
        });
        var handler = new S2SHeaderHandler(options)
        {
            InnerHandler = new TestHttpMessageHandler()
        };
        using var invoker = new HttpMessageInvoker(handler);
        var allowedRequest = new HttpRequestMessage(HttpMethod.Get, "https://api.internal/test");
        allowedRequest.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", "stale");
        allowedRequest.Headers.TryAddWithoutValidation(CustomRequestHeaders.InternalApiKey, "stale");

        await invoker.SendAsync(allowedRequest, CancellationToken.None);

        Assert.Equal("TestS2SToken", allowedRequest.Headers.GetValues(CustomRequestHeaders.S2SToken).Single());
        Assert.Null(allowedRequest.Headers.Authorization);
        Assert.False(allowedRequest.Headers.Contains(CustomRequestHeaders.InternalApiKey));

        var ipv6Request = new HttpRequestMessage(HttpMethod.Get, "https://[::1]/test");
        await invoker.SendAsync(ipv6Request, CancellationToken.None);
        Assert.True(ipv6Request.Headers.Contains(CustomRequestHeaders.S2SToken));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            invoker.SendAsync(
                new HttpRequestMessage(HttpMethod.Get, "http://api.internal/test"),
                CancellationToken.None));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            invoker.SendAsync(
                new HttpRequestMessage(HttpMethod.Get, "https://other.internal/test"),
                CancellationToken.None));
    }

    [Fact]
    public async Task S2SHeaderHandler_EmptyConfiguredToken_ShouldRemoveCallerSuppliedManagedCredentials()
    {
        var handler = new S2SHeaderHandler(Options.Create(new S2SAuthOptions { Token = string.Empty }))
        {
            InnerHandler = new TestHttpMessageHandler()
        };
        using var invoker = new HttpMessageInvoker(handler);
        var request = new HttpRequestMessage(HttpMethod.Get, "http://localhost/test");
        request.Headers.TryAddWithoutValidation(CustomRequestHeaders.S2SToken, "caller-token");
        request.Headers.TryAddWithoutValidation(CustomRequestHeaders.InternalApiKey, "caller-token");
        request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", "caller-token");

        await invoker.SendAsync(request, CancellationToken.None);

        Assert.False(request.Headers.Contains(CustomRequestHeaders.S2SToken));
        Assert.False(request.Headers.Contains(CustomRequestHeaders.InternalApiKey));
        Assert.Null(request.Headers.Authorization);
    }

    [Fact]
    public void AddS2SAuth_UnknownPrimaryHandler_ShouldFailClosedAgainstRedirectLeakage()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["S2S:Token"] = "valid-test-token"
            })
            .Build();
        services.AddS2SOnly(configuration);
        services.AddHttpClient("unsafe-before")
            .ConfigurePrimaryHttpMessageHandler(() => new TestHttpMessageHandler())
            .AddS2SAuth();
        services.AddHttpClient("unsafe-after")
            .AddS2SAuth()
            .ConfigurePrimaryHttpMessageHandler(() => new TestHttpMessageHandler());
        using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IHttpClientFactory>();

        Assert.Throws<InvalidOperationException>(() => factory.CreateClient("unsafe-before"));
        Assert.Throws<InvalidOperationException>(() => factory.CreateClient("unsafe-after"));
    }

    [Fact]
    public void AddCorsPolicy_ShouldUseIsolatedSafeBrowserPolicy()
    {
        var services = new ServiceCollection();
        services.AddCorsPolicy(options =>
        {
            options.AllowedOrigins.Add("https://portal.example.com");
            options.AllowedHeaders.Add("x-correlation-id");
        });
        using var provider = services.BuildServiceProvider();
        var corsOptions = provider.GetRequiredService<IOptions<CorsOptions>>().Value;
        var policy = corsOptions.GetPolicy("DefaultPolicy");

        Assert.NotNull(policy);
        Assert.Equal(["https://portal.example.com"], policy.Origins);
        Assert.Contains("x-correlation-id", policy.Headers, StringComparer.OrdinalIgnoreCase);

        Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddCorsPolicy(options =>
            options.AllowedOrigins.Add("https://portal.example.com/path")));
        Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddCorsPolicy(options =>
        {
            options.AllowedOrigins.Add("https://portal.example.com");
            options.AllowedHeaders.Add("X-S2S-Token");
        }));
        Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddCorsPolicy(options =>
            options.AllowedOrigins = null!));
    }

    [Fact]
    public void AuthOptionsValidators_ShouldValidateJwtAndS2SProperly()
    {
        var s2SResult = new S2SAuthOptionsValidator().Validate(null, new S2SAuthOptions
        {
            Token = "bounded-token",
            DestinationValidation = S2SDestinationValidationMode.Strict,
            AllowedHosts = []
        });
        var jwtResult = new JwtBearerAuthOptionsValidator().Validate(null, new JwtBearerAuthOptions
        {
            Authority = "http://insecure.example.com",
            RequireHttpsMetadata = true
        });

        Assert.True(s2SResult.Failed);
        Assert.True(jwtResult.Failed);

        var validJwtResult = new JwtBearerAuthOptionsValidator().Validate(null, new JwtBearerAuthOptions
        {
            Authority = "https://auth.example.com"
        });
        Assert.False(validJwtResult.Failed);

        var ipv6Result = new S2SAuthOptionsValidator().Validate(null, new S2SAuthOptions
        {
            Token = "bounded-token",
            DestinationValidation = S2SDestinationValidationMode.Strict,
            AllowedHosts = ["::1"]
        });
        Assert.False(ipv6Result.Failed);

        var emptyTokenResult = new S2SAuthOptionsValidator().Validate(null, new S2SAuthOptions());
        Assert.True(emptyTokenResult.Failed);
    }

    private static ClaimsPrincipal CreateServicePrincipal()
    {
        var identity = new ClaimsIdentity(
            [
                new Claim(ClaimTypes.Name, "S2S"),
                new Claim("scope", "internal"),
                new Claim("identity_type", "service")
            ],
            S2SAuthOptions.SchemeName);
        return new ClaimsPrincipal(identity);
    }

    private class TestHttpMessageHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.OK));
        }
    }
}
