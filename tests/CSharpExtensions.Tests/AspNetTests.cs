using CSharpExtensions.AspNetCore.AspNet.Configurations;
using CSharpExtensions.AspNetCore.AspNet.Extensions;
using CSharpExtensions.AspNetCore.AspNet.Handlers;
using CSharpExtensions.AspNetCore.AspNet.Profiles;
using CSharpExtensions.AspNetCore.AspNet.Transformers;
using CSharpExtensions.Core.Exceptions.Exceptions;
using CSharpExtensions.Core.Railway;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace CSharpExtensions.Tests;

public class AspNetTests
{
    private readonly Mock<IHttpContextAccessor> _httpContextAccessorMock;
    private readonly IActionResultProfile _profile;

    public AspNetTests()
    {
        _httpContextAccessorMock = new Mock<IHttpContextAccessor>();
        _httpContextAccessorMock.Setup(x => x.HttpContext).Returns(new DefaultHttpContext());
        
        _profile = new ActionResultProfile(_httpContextAccessorMock.Object);
        
        RailwayConfiguration.Setup(s => s.CurrentProfile = _profile);
    }

    [Fact]
    public void DefaultResultTransformer_SuccessResult_ShouldReturnOk()
    {
        var transformer = new DefaultResultTransformer();
        var result = Result.Success("Success Data");

        var actionResult = transformer.Transform(result, _profile);

        var okResult = Assert.IsType<OkObjectResult>(actionResult.Result);
        Assert.Equal("Success Data", okResult.Value);
    }

    [Fact]
    public void DefaultResultTransformer_FailureResult_ShouldReturnProblemDetails()
    {
        var transformer = new DefaultResultTransformer();
        var error = new Error("Bad thing happened").AsBadRequest("BadType", "Bad Title");
        var result = Result.Failure(error);

        var actionResult = transformer.Transform(result, _profile);

        var problemResult = Assert.IsAssignableFrom<ObjectResult>(actionResult);
        Assert.Equal(400, problemResult.StatusCode);
        var problem = Assert.IsType<ProblemDetails>(problemResult.Value);
        Assert.Equal("Bad Title", problem.Title);
        Assert.Equal("Bad thing happened", problem.Detail);
    }

    [Fact]
    public async Task ApiExceptionHandler_ShouldHandleApiException()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<ApiExceptionHandler>>();
        var transformer = new DefaultResultTransformer();
        var handler = new ApiExceptionHandler(loggerMock.Object, transformer);
        
        using var serviceProvider = CreateMvcServiceProvider();
        var httpContext = new DefaultHttpContext { RequestServices = serviceProvider };
        httpContext.Response.Body = new MemoryStream(); // To support JSON writing
        _httpContextAccessorMock.Setup(x => x.HttpContext).Returns(httpContext);
        
        var exception = new BadRequestException("Invalid input", "ValidationFailed", "Validation Error");

        // Act
        var result = await handler.TryHandleAsync(httpContext, exception, CancellationToken.None);

        // Assert
        Assert.True(result);
        Assert.Equal(400, httpContext.Response.StatusCode);
    }

    [Fact]
    public async Task ApiExceptionHandler_ShouldHandleStandardExceptions()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<ApiExceptionHandler>>();
        var transformer = new DefaultResultTransformer();
        var handler = new ApiExceptionHandler(loggerMock.Object, transformer);
        
        using var serviceProvider = CreateMvcServiceProvider();
        var httpContext = new DefaultHttpContext { RequestServices = serviceProvider };
        httpContext.Response.Body = new MemoryStream();
        _httpContextAccessorMock.Setup(x => x.HttpContext).Returns(httpContext);
        
        var exception = new UnauthorizedAccessException("No access");

        // Act
        var result = await handler.TryHandleAsync(httpContext, exception, CancellationToken.None);

        // Assert
        Assert.True(result);
        Assert.Equal(401, httpContext.Response.StatusCode);
    }

    [Fact]
    public async Task ApiExceptionHandler_ShouldExecuteNonObjectTransformerResult()
    {
        var loggerMock = new Mock<ILogger<ApiExceptionHandler>>();
        var transformer = new StatusCodeResultTransformer(StatusCodes.Status418ImATeapot);
        var handler = new ApiExceptionHandler(loggerMock.Object, transformer);
        using var serviceProvider = CreateMvcServiceProvider();
        var httpContext = new DefaultHttpContext { RequestServices = serviceProvider };
        httpContext.Response.Body = new MemoryStream();
        _httpContextAccessorMock.Setup(x => x.HttpContext).Returns(httpContext);

        var handled = await handler.TryHandleAsync(
            httpContext,
            new InvalidOperationException("Sensitive downstream detail"),
            CancellationToken.None);

        Assert.True(handled);
        Assert.Equal(StatusCodes.Status418ImATeapot, httpContext.Response.StatusCode);
    }

    [Fact]
    public void UseRailwayWithApiExceptions_ShouldConfigureRegisteredTransformer()
    {
        var transformer = new TrackingResultTransformer();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IResultTransformer>(transformer);
        services.AddRailwayWithApiExceptions();
        using var serviceProvider = services.BuildServiceProvider();
        var application = new ApplicationBuilder(serviceProvider);

        try
        {
            application.UseRailwayWithApiExceptions();

            var actionResult = Result.Success("Data").ToActionResult();

            Assert.Same(transformer, RailwayConfiguration.GetCurrentTransformer());
            Assert.Equal(1, transformer.GenericTransformCalls);
            var okResult = Assert.IsType<OkObjectResult>(actionResult.Result);
            Assert.Equal("custom-transformer", okResult.Value);

            var errorResult = new Error("Failure").ToActionResult();

            Assert.Equal(1, transformer.NonGenericTransformCalls);
            var transformedError = Assert.IsType<OkObjectResult>(errorResult);
            Assert.Equal("custom-transformer", transformedError.Value);
        }
        finally
        {
            RailwayConfiguration.Setup(settings =>
            {
                settings.CurrentProfile = _profile;
                settings.CurrentTransformer = new DefaultResultTransformer();
            });
        }
    }

    [Theory]
    [InlineData(409)]
    [InlineData(502)]
    public void ProblemDetailsRoundTrip_ShouldPreserveSafeHttpStatus(int statusCode)
    {
        var source = new ProblemDetails
        {
            Status = statusCode,
            Type = "Test.Error",
            Title = "Test error",
            Detail = "The operation failed."
        };

        var error = source.ToError();
        var actionResult = error.ToActionResult(_profile);

        Assert.Equal(statusCode, error.HttpStatusCode);
        var objectResult = Assert.IsType<ObjectResult>(actionResult);
        Assert.Equal(statusCode, objectResult.StatusCode);
    }

    private sealed class TrackingResultTransformer : IResultTransformer
    {
        public int NonGenericTransformCalls { get; private set; }
        public int GenericTransformCalls { get; private set; }

        public ActionResult Transform(Result result, IActionResultProfile profile)
        {
            NonGenericTransformCalls++;
            return new OkObjectResult("custom-transformer");
        }

        public ActionResult<TValue> Transform<TValue>(Result<TValue> result, IActionResultProfile profile)
        {
            GenericTransformCalls++;
            return new OkObjectResult("custom-transformer");
        }
    }

    private static ServiceProvider CreateMvcServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddControllers();
        return services.BuildServiceProvider();
    }

    private sealed class StatusCodeResultTransformer(int statusCode) : IResultTransformer
    {
        public ActionResult Transform(Result result, IActionResultProfile profile)
        {
            return new StatusCodeResult(statusCode);
        }

        public ActionResult<TValue> Transform<TValue>(Result<TValue> result, IActionResultProfile profile)
        {
            return new StatusCodeResult(statusCode);
        }
    }
}
