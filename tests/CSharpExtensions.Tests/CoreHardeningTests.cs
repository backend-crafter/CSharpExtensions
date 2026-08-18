using System.Buffers;
using System.Text;
using System.Text.Json;
using CSharpExtensions.AspNetCore.AspNet.Middleware;
using CSharpExtensions.Foundation.Helpers.Constants;
using CSharpExtensions.Foundation.Helpers.Extensions;
using CSharpExtensions.Foundation.Helpers.Models;
using CSharpExtensions.Foundation.Helpers.Options;
using CSharpExtensions.Foundation.Json;
using CSharpExtensions.Foundation.Json.Enums;
using CSharpExtensions.Foundation.Json.Extensions;
using CSharpExtensions.Foundation.Railway;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Primitives;
using Xunit;
using ExceptionExtensions = CSharpExtensions.AspNetCore.AspNet.Extensions.ExceptionExtensions;

namespace CSharpExtensions.Tests;

public sealed class CoreHardeningTests
{

    [Fact]
    public void PagedList_ShouldRejectInvalidPagingMetadata()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new PagedList<int>([], 0, 10, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PagedList<int>([], 1, 0, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PagedList<int>([], 1, 10, -1));
    }

    [Fact]
    public void PagedList_TotalPages_ShouldBeOverflowSafe()
    {
        var page = new PagedList<int>([], 1, 1, long.MaxValue);

        Assert.Equal(int.MaxValue, page.TotalPages);
    }

    [Fact]
    public void PagedList_WithExpression_ShouldPreserveValidatedMetadata()
    {
        var page = new PagedList<int>([1], 1, 10, 1);

        Assert.Throws<ArgumentOutOfRangeException>(() => _ = page with { PageNumber = 0 });
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = page with { PageSize = 0 });
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = page with { TotalCount = -1 });
        Assert.Throws<ArgumentNullException>(() => _ = page with { Items = null! });
    }

    [Fact]
    public void CursorPagedList_WithExpression_ShouldPreserveCrossPropertyInvariants()
    {
        var page = new CursorPagedList<int, int>([1], 1, false, default);

        Assert.Throws<ArgumentOutOfRangeException>(() => _ = page with { PageSize = -1 });
        Assert.Throws<ArgumentException>(() => _ = page with { PageSize = 0 });
        Assert.Throws<ArgumentException>(() => _ = page with { Items = [], HasMore = true });
        Assert.Throws<ArgumentException>(() => _ = page with { HasMore = true, Items = [] });
        Assert.Throws<ArgumentNullException>(() => _ = page with { Items = null! });
    }

    [Fact]
    public void StringSlices_ShouldRejectNegativeLengths()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => "value".Left(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => "value".Right(int.MinValue));
    }

    [Fact]
    public void AddUrlSegment_ShouldRejectDestinationReplacement()
    {
        var baseAddress = new Uri("https://api.example.test/root/");

        Assert.Throws<ArgumentException>(() => baseAddress.AddUrlSegment("https://attacker.example/path"));
        Assert.Throws<ArgumentException>(() => baseAddress.AddUrlSegment("//attacker.example/path"));
        Assert.Throws<ArgumentException>(() => baseAddress.AddUrlSegment("../admin"));
        Assert.Throws<ArgumentException>(() => baseAddress.AddUrlSegment("%2e%2e/admin"));
        Assert.Throws<ArgumentException>(() => baseAddress.AddUrlSegment("child\\admin"));

        var result = baseAddress.AddUrlSegment("users/42?include=summary");

        Assert.Equal("https://api.example.test/root/users/42?include=summary", result.AbsoluteUri);
    }

    [Fact]
    public void EnumParsing_ShouldRejectUndefinedNumericValues()
    {
        Assert.Null("99".ToNullableEnum<DayOfWeek>());
        Assert.Throws<ArgumentException>(() => "99".ToEnum<DayOfWeek>());
    }

    [Fact]
    public void SharedJsonOptions_ShouldBeReadOnly()
    {
        Assert.True(JsonOptions.Default.IsReadOnly);
        Assert.Throws<InvalidOperationException>(() => JsonOptions.Default.WriteIndented = true);
    }

    [Fact]
    public void StrictJsonProfile_ShouldRejectUnknownMembers()
    {
        Assert.Throws<JsonException>(() =>
            JsonSerializer.Deserialize<StrictModel>("{\"value\":1,\"unexpected\":2}", JsonOptions.ExternalStrict));
    }

    [Fact]
    public void StrictJsonProfile_ShouldRejectNumericEnumValues()
    {
        Assert.Throws<JsonException>(() =>
            JsonSerializer.Deserialize<StrictEnumModel>("{\"state\":1}", JsonOptions.ExternalStrict));
    }

    [Fact]
    public void SafeJsonDeserialization_ShouldRejectAmbiguousPropertyNames()
    {
        var json = "{\"value\":1,\"Value\":2}"u8;

        var success = json.TryDeserializeSafe<StrictModel>(out _);

        Assert.False(success);
    }

    [Fact]
    public void JsonUnion_ShouldTreatObjectPropertyOrderAsEquivalent()
    {
        var result = JsonExtensions.Merge(
            "[{\"a\":1,\"b\":2}]",
            "[{\"b\":2,\"a\":1}]",
            JsonMergeHandling.Union);

        using var document = JsonDocument.Parse(result);
        Assert.Equal(1, document.RootElement.GetArrayLength());
    }

    [Fact]
    public void JsonUnion_ShouldRemoveDuplicatesAlreadyPresentInTarget()
    {
        var result = JsonExtensions.Merge(
            "[{\"a\":1},{\"a\":1}]",
            "[{\"a\":1}]",
            JsonMergeHandling.Union);

        using var document = JsonDocument.Parse(result);
        Assert.Equal(1, document.RootElement.GetArrayLength());
    }

    [Fact]
    public void JsonMerge_WithBlankSide_ShouldStillValidateTheOtherInput()
    {
        Assert.ThrowsAny<JsonException>(() =>
            JsonExtensions.Merge(string.Empty, "not-json"));
    }

    [Fact]
    public void JsonMerge_ShouldRejectUndefinedArrayHandlingAtEveryPublicEntry()
    {
        var invalidHandling = (JsonMergeHandling)int.MaxValue;
        var arrayJson = Encoding.UTF8.GetBytes("[]");
        using var document = JsonDocument.Parse(arrayJson);
        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            JsonExtensions.Merge(string.Empty, "[]", invalidHandling));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            JsonMerger.Merge(arrayJson, arrayJson, invalidHandling));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            JsonMerger.Merge(writer, document.RootElement, document.RootElement, invalidHandling));
    }

    [Fact]
    public async Task HttpResultReader_ShouldRejectOversizedBodiesWithoutExposingContent()
    {
        using var content = new ByteArrayContent(Encoding.UTF8.GetBytes("{\"secret\":\"value\"}"));

        var result = await content.ReadAsResultAsync<StrictModel>(maximumResponseBytes: 4);

        Assert.True(result.IsFailure);
        Assert.DoesNotContain("secret", result.Error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task HttpResultReader_ShouldRejectUnsupportedConfiguredLimit()
    {
        using var content = new StringContent("{}");

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            content.ReadAsResultAsync<StrictModel>(int.MaxValue));
    }

    [Fact]
    public async Task HttpResultReader_ShouldAllowUnknownMembersForForwardCompatibility()
    {
        using var content = new StringContent(
            "{\"value\":1,\"state\":\"active\",\"futureField\":\"ignored\"}",
            Encoding.UTF8,
            "application/json");

        var result = await content.ReadAsResultAsync<HttpResponseModel>();

        Assert.True(result.IsSuccess);
        Assert.Equal(1, result.Value.Value);
        Assert.Equal(StrictState.Active, result.Value.State);
    }

    [Theory]
    [InlineData("{\"value\":\"1\",\"state\":\"active\"}")]
    [InlineData("{\"value\":1,\"state\":1}")]
    [InlineData("{\"value\":1,\"state\":\"active\",}")]
    [InlineData("{\"value\":1/*comment*/,\"state\":\"active\"}")]
    [InlineData("{\"value\":1,\"Value\":2,\"state\":\"active\"}")]
    public async Task HttpResultReader_ShouldRejectAmbiguousOrNonCanonicalJson(string json)
    {
        using var content = new StringContent(json, Encoding.UTF8, "application/json");

        var result = await content.ReadAsResultAsync<HttpResponseModel>();

        Assert.True(result.IsFailure);
        Assert.Equal("RemoteResponseInvalid", result.Error.Type);
    }

    [Fact]
    public async Task HttpResultReader_ShouldHonorExplicitSerializerOptions()
    {
        using var content = new StringContent(
            "{\"value\":1,\"state\":\"active\",\"futureField\":\"rejected\"}",
            Encoding.UTF8,
            "application/json");

        var result = await content.ReadAsResultAsync<HttpResponseModel>(JsonOptions.ExternalStrict);

        Assert.True(result.IsFailure);
        Assert.Equal("RemoteResponseInvalid", result.Error.Type);
    }

    [Fact]
    public void UnknownExceptions_ShouldProduceGenericProblemDetails()
    {
        var context = new DefaultHttpContext();

        var details = ExceptionExtensions.CreateProblemDetails(
            context,
            new InvalidOperationException("database-password-value"));

        Assert.Equal(StatusCodes.Status500InternalServerError, details.Status);
        Assert.DoesNotContain("database-password-value", details.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void RailwayErrorDiagnostics_ShouldNotRenderExceptionMessages()
    {
        var error = new Error("safe public message")
            .CausedBy(new InvalidOperationException("database-password-value"));

        Assert.Empty(error.Details);
        Assert.DoesNotContain("database-password-value", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultResult_ShouldExposeAStableFailureInsteadOfNullError()
    {
        Result result = default;
        Result<int> valueResult = default;

        Assert.True(result.IsFailure);
        Assert.Same(Error.Uninitialized, result.Error);
        Assert.Same(result.Error, result.Error);
        Assert.True(valueResult.IsFailure);
        Assert.Same(Error.Uninitialized, valueResult.Error);
        Assert.Same(valueResult.Error, valueResult.Error);
        Assert.Equal(DateTime.MinValue, Error.Uninitialized.Timestamp);
    }

    [Fact]
    public void SentinelErrors_ShouldRejectFluentMutation()
    {
        Assert.Throws<InvalidOperationException>(() => Error.None.AsNotFound());
        Assert.Throws<InvalidOperationException>(() => Error.Uninitialized.WithDetails("ignored"));
        Assert.Throws<InvalidOperationException>(() => Error.Uninitialized.WithMetadata("key", "value"));
        Assert.Empty(Error.None.Details);
        Assert.Empty(Error.Uninitialized.Metadata);
    }

    [Fact]
    public async Task Retry_ShouldNeverRepeatCancellation()
    {
        var attempts = 0;
        Func<CancellationToken, Task<int>> operation = _ =>
        {
            attempts++;
            throw new OperationCanceledException();
        };

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            operation.TryAgainAsync(3, TimeSpan.Zero));
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task CorrelationMiddleware_ShouldReplaceAmbiguousInboundValues()
    {
        var context = new DefaultHttpContext();
        context.Request.Headers[CustomRequestHeaders.CorrelationId] =
            new StringValues(["first", "second"]);
        string? observed = null;
        var middleware = new CorrelationIdMiddleware(nextContext =>
        {
            observed = nextContext.Request.Headers[CustomRequestHeaders.CorrelationId].ToString();
            return Task.CompletedTask;
        });

        await middleware.InvokeAsync(context);

        Assert.NotNull(observed);
        Assert.DoesNotContain("first", observed, StringComparison.Ordinal);
        Assert.Equal(observed, context.Items[CustomRequestHeaders.CorrelationId]);
    }

    [Fact]
    public void ShardingValidator_ShouldRejectNonPowerOfTwoTopology()
    {
        var result = new ShardingOptionsValidator().Validate(
            null,
            new ShardingOptions { LogicalShardCount = 127 });

        Assert.True(result.Failed);
    }

    [Fact]
    public void DatabasesValidator_ShouldAllowAbsentDatabaseSections()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Databases:Integration:ConnectionString"] = "Server=integration;Database=main"
            })
            .Build();
        var options = new DatabasesOptions
        {
            ["Integration"] = new DatabaseOptions
            {
                ConnectionString = "Server=integration;Database=main"
            }
        };

        var result = new DatabasesOptionsValidator(configuration).Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void DatabasesValidator_ShouldRejectMalformedConfiguredSection()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Databases:Redis:ConnectionString"] = string.Empty
            })
            .Build();

        var result = new DatabasesOptionsValidator(configuration).Validate(null, new DatabasesOptions());

        Assert.True(result.Failed);
    }

    private sealed record StrictModel(int Value);

    private sealed record HttpResponseModel(int Value, StrictState State);

    private sealed record StrictEnumModel(StrictState State);

    private enum StrictState
    {
        Unknown,
        Active
    }
}
