using System.Buffers;
using System.Text.Json;
using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Core.Json.Extensions;

/// <summary>
/// Provides Result-aware extensions for <see cref="HttpClient"/> and <see cref="HttpContent"/>.
/// </summary>
public static class HttpClientExtensions
{
    private const int DefaultMaximumResponseBytes = 1024 * 1024;
    private const int MaximumSupportedResponseBytes = 16 * 1024 * 1024;

    /// <summary>
    /// Reads the content as a <see cref="Result{TValue}"/> using standard JSON conventions.
    /// </summary>
    public static async Task<Result<TValue>> ReadAsResultAsync<TValue>(this HttpContent content, CancellationToken cancellationToken = default)
    {
        return await ReadAsResultAsync<TValue>(
            content,
            DefaultMaximumResponseBytes,
            JsonOptions.HttpResponse,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the content with explicit serializer options while retaining structural safety checks.
    /// </summary>
    public static async Task<Result<TValue>> ReadAsResultAsync<TValue>(
        this HttpContent content,
        JsonSerializerOptions serializerOptions,
        CancellationToken cancellationToken = default)
    {
        return await ReadAsResultAsync<TValue>(
            content,
            DefaultMaximumResponseBytes,
            serializerOptions,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a bounded response body as a result using standard JSON conventions.
    /// </summary>
    public static async Task<Result<TValue>> ReadAsResultAsync<TValue>(
        this HttpContent content,
        int maximumResponseBytes,
        CancellationToken cancellationToken = default)
    {
        return await ReadAsResultAsync<TValue>(
            content,
            maximumResponseBytes,
            JsonOptions.HttpResponse,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a bounded response body using explicit serializer options and mandatory structural validation.
    /// </summary>
    public static async Task<Result<TValue>> ReadAsResultAsync<TValue>(
        this HttpContent content,
        int maximumResponseBytes,
        JsonSerializerOptions serializerOptions,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);
        ArgumentNullException.ThrowIfNull(serializerOptions);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumResponseBytes, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maximumResponseBytes, MaximumSupportedResponseBytes);

        try
        {
            if (content.Headers.ContentLength is long contentLength && contentLength > maximumResponseBytes)
            {
                return CreateInvalidResponse<TValue>("RemoteResponseTooLarge", "Remote response exceeded the configured limit.");
            }

            var body = await ReadBoundedAsync(content, maximumResponseBytes, cancellationToken).ConfigureAwait(false);
            if (body.Length == 0)
            {
                return CreateInvalidResponse<TValue>("RemoteResponseEmpty", "Remote response was empty.");
            }

            var parsed = JsonExtensions.TryDeserializeSafe<TValue>(
                body.AsSpan(),
                out var value,
                serializerOptions);
            return parsed && value is not null
                ? Result.Success(value)
                : CreateInvalidResponse<TValue>("RemoteResponseInvalid", "Remote response could not be parsed.");
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            return CreateInvalidResponse<TValue>("RemoteResponseInvalid", "Remote response could not be parsed.");
        }
        catch (InvalidDataException)
        {
            return CreateInvalidResponse<TValue>("RemoteResponseTooLarge", "Remote response exceeded the configured limit.");
        }
        catch (IOException)
        {
            return CreateInvalidResponse<TValue>("RemoteResponseReadError", "Remote response could not be read.");
        }
    }

    /// <summary>
    /// Sends a GET request and returns the response as a <see cref="Result{TValue}"/>.
    /// </summary>
    public static async Task<Result<TValue>> GetAsResultAsync<TValue>(this HttpClient client, string requestUri, CancellationToken cancellationToken = default)
    {
        return await GetAsResultAsync<TValue>(
            client,
            requestUri,
            JsonOptions.HttpResponse,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends a GET request and reads the response using explicit serializer options.
    /// </summary>
    public static async Task<Result<TValue>> GetAsResultAsync<TValue>(
        this HttpClient client,
        string requestUri,
        JsonSerializerOptions serializerOptions,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(requestUri);
        ArgumentNullException.ThrowIfNull(serializerOptions);

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, requestUri);
            using var response = await client.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                return new Error("Remote service returned an unsuccessful response.")
                    .AsInternalServer("RemoteServiceError", "Remote service returned an error.")
                    .WithMetadata("StatusCode", (int)response.StatusCode);
            }

            return await response.Content.ReadAsResultAsync<TValue>(
                DefaultMaximumResponseBytes,
                serializerOptions,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (HttpRequestException)
        {
            return new Error("Remote service request failed.")
                .AsInternalServer("RemoteServiceUnavailable", "Failed to communicate with remote service.");
        }
    }

    private static async Task<byte[]> ReadBoundedAsync(
        HttpContent content,
        int maximumResponseBytes,
        CancellationToken cancellationToken)
    {
        await using var source = await content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using var destination = new MemoryStream(Math.Min(maximumResponseBytes, 81920));
        var buffer = ArrayPool<byte>.Shared.Rent(81920);

        try
        {
            while (true)
            {
                var bytesRead = await source.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false);
                if (bytesRead == 0)
                {
                    break;
                }

                if (destination.Length + bytesRead > maximumResponseBytes)
                {
                    throw new InvalidDataException("Response body limit exceeded.");
                }

                await destination.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken).ConfigureAwait(false);
            }

            return destination.ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    private static Result<TValue> CreateInvalidResponse<TValue>(string type, string title)
    {
        return new Error("Remote response was invalid.")
            .AsInternalServer(type, title);
    }
}
