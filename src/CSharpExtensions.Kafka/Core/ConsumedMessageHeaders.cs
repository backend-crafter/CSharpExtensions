using CSharpExtensions.Core.Helpers.Constants;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using Confluent.Kafka;

/// <summary>
/// Holds the extracted header values from a Kafka consume result.
/// Consolidates header parsing logic that was previously duplicated across handler and channel consumer loops.
/// </summary>
internal sealed class ConsumedMessageHeaders
{
    private const int MaximumHeaderCount = 64;
    private const int MaximumHeaderKeyByteCount = 128;
    private const int MaximumHeaderValueByteCount = 8 * 1024;
    private const int MaximumTotalHeaderByteCount = 32 * 1024;
    private const int MaximumMessageIdByteCount = 256;
    private const int MaximumCorrelationIdByteCount = 256;
    private const int MaximumSchemaVersionByteCount = 256;
    private const int MaximumSignatureByteCount = 2048;
    private const int MaximumTraceparentByteCount = 128;

    private static readonly HashSet<string> ProtectedHeaderNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "traceparent",
        CustomRequestHeaders.MessageId,
        CustomRequestHeaders.CorrelationId,
        CustomRequestHeaders.EventSchemaVersion,
        CustomRequestHeaders.MessageSignature
    };

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new ReadOnlyDictionary<string, string>(new Dictionary<string, string>());

    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    /// <summary>
    /// The W3C traceparent header for distributed tracing propagation.
    /// </summary>
    public string? Traceparent { get; init; }

    /// <summary>
    /// The unique message identifier extracted from headers, or an optional generated fallback.
    /// </summary>
    public required string MessageId { get; init; }

    /// <summary>
    /// Indicates whether exactly one valid x-message-id header was supplied by the producer.
    /// </summary>
    public required bool HasValidMessageIdHeader { get; init; }

    /// <summary>
    /// The correlation identifier for distributed tracing, or a generated GUID if absent.
    /// </summary>
    public required string CorrelationId { get; init; }

    /// <summary>
    /// The schema version key used for message upcasting resolution.
    /// </summary>
    public required string SchemaVersionKey { get; init; }

    /// <summary>
    /// The encrypted SHA-256 message digest, if present.
    /// </summary>
    public string? Signature { get; init; }

    /// <summary>
    /// The full dictionary of all raw header key-value pairs.
    /// Used by channel-mode consumers to populate <see cref="CSharpExtensions.Kafka.Abstractions.ConsumeContext{TMessage}"/>.
    /// Empty in handler-mode to avoid unnecessary allocation.
    /// </summary>
    public required IReadOnlyDictionary<string, string> RawHeaders { get; init; }

    /// <summary>
    /// Extracts and parses all known Kafka message headers from a consume result.
    /// </summary>
    /// <typeparam name="TMessage">The message type, used as the default schema version key fallback.</typeparam>
    /// <param name="consumeResult">The raw Kafka consume result.</param>
    /// <param name="collectRawHeaders">
    /// When <c>true</c>, all headers are collected into <see cref="RawHeaders"/>.
    /// When <c>false</c>, <see cref="RawHeaders"/> is set to an empty dictionary to avoid allocation.
    /// Channel-mode requires raw headers for <see cref="CSharpExtensions.Kafka.Abstractions.ConsumeContext{TMessage}"/>;
    /// handler-mode does not.
    /// </param>
    /// <param name="allowGeneratedMessageIdFallback">
    /// When <c>true</c>, a GUID is generated when x-message-id is missing or invalid.
    /// Idempotent consumers must pass <c>false</c> so an invalid producer identity cannot be
    /// converted into a process-local identity.
    /// </param>
    /// <returns>A fully populated <see cref="ConsumedMessageHeaders"/> instance.</returns>
    public static ConsumedMessageHeaders Extract<TMessage>(
        ConsumeResult<string, string> consumeResult,
        bool collectRawHeaders = false,
        bool allowGeneratedMessageIdFallback = true)
    {
        var headers = consumeResult.Message.Headers ?? new Headers();
        if (headers.Count > MaximumHeaderCount)
        {
            throw new InvalidDataException($"Kafka message contains more than {MaximumHeaderCount} headers.");
        }

        var headerDictionary = collectRawHeaders
            ? new Dictionary<string, string>(headers.Count, StringComparer.OrdinalIgnoreCase)
            : null;
        var observedProtectedHeaders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var totalHeaderByteCount = 0;

        string? traceparent = null;
        string? messageId = null;
        var hasValidMessageIdHeader = false;
        string correlationId = Guid.NewGuid().ToString();
        string schemaVersionKey = typeof(TMessage).Name;
        string? signature = null;

        foreach (var header in headers)
        {
            if (string.IsNullOrWhiteSpace(header.Key))
            {
                throw new InvalidDataException("Kafka message contains an empty header key.");
            }

            if (!IsSafeHeaderKey(header.Key))
            {
                throw new InvalidDataException("Kafka message contains an invalid header key.");
            }

            int keyByteCount;
            try
            {
                keyByteCount = StrictUtf8.GetByteCount(header.Key);
            }
            catch (EncoderFallbackException exception)
            {
                throw new InvalidDataException("Kafka message contains a header key with invalid Unicode data.", exception);
            }
            var valueBytes = header.GetValueBytes() ?? Array.Empty<byte>();
            if (keyByteCount > MaximumHeaderKeyByteCount || valueBytes.Length > MaximumHeaderValueByteCount)
            {
                throw new InvalidDataException($"Kafka header '{header.Key}' exceeds the configured size limit.");
            }

            totalHeaderByteCount = checked(totalHeaderByteCount + keyByteCount + valueBytes.Length);
            if (totalHeaderByteCount > MaximumTotalHeaderByteCount)
            {
                throw new InvalidDataException($"Kafka message headers exceed {MaximumTotalHeaderByteCount} bytes.");
            }

            if (ProtectedHeaderNames.Contains(header.Key) && !observedProtectedHeaders.Add(header.Key))
            {
                throw new InvalidDataException($"Kafka message contains duplicate protected header '{header.Key}'.");
            }

            var valueString = DecodeStrictUtf8(header.Key, valueBytes);
            if (headerDictionary is not null)
            {
                headerDictionary[header.Key] = valueString;
            }

            if (string.Equals(header.Key, "traceparent", StringComparison.OrdinalIgnoreCase))
            {
                EnsureMaximumSize(header.Key, valueBytes, MaximumTraceparentByteCount);
                if (!ActivityContext.TryParse(valueString, null, out _))
                {
                    throw new InvalidDataException("Kafka traceparent header is not a valid W3C trace context.");
                }

                traceparent = valueString;
            }
            else if (string.Equals(header.Key, CustomRequestHeaders.MessageId, StringComparison.OrdinalIgnoreCase))
            {
                if (!TryDecodeSafeIdentifier(valueBytes, MaximumMessageIdByteCount, out var decodedMessageId))
                {
                    throw new InvalidDataException("Kafka message identifier header is invalid.");
                }

                hasValidMessageIdHeader = true;
                messageId = decodedMessageId;
            }
            else if (string.Equals(header.Key, CustomRequestHeaders.CorrelationId, StringComparison.OrdinalIgnoreCase))
            {
                if (!TryDecodeSafeIdentifier(valueBytes, MaximumCorrelationIdByteCount, out correlationId))
                {
                    throw new InvalidDataException("Kafka correlation identifier header is invalid.");
                }
            }
            else if (string.Equals(header.Key, CustomRequestHeaders.EventSchemaVersion, StringComparison.OrdinalIgnoreCase))
            {
                EnsureMaximumSize(header.Key, valueBytes, MaximumSchemaVersionByteCount);
                if (!IsSafeSchemaKey(valueString))
                {
                    throw new InvalidDataException("Kafka schema version header is invalid.");
                }

                schemaVersionKey = valueString;
            }
            else if (string.Equals(header.Key, CustomRequestHeaders.MessageSignature, StringComparison.OrdinalIgnoreCase))
            {
                EnsureMaximumSize(header.Key, valueBytes, MaximumSignatureByteCount);
                if (!IsSafeOpaqueValue(valueString))
                {
                    throw new InvalidDataException("Kafka message signature header is invalid.");
                }

                signature = valueString;
            }
        }

        return new ConsumedMessageHeaders
        {
            Traceparent = traceparent,
            MessageId = hasValidMessageIdHeader
                ? messageId!
                : allowGeneratedMessageIdFallback ? Guid.NewGuid().ToString() : string.Empty,
            HasValidMessageIdHeader = hasValidMessageIdHeader,
            CorrelationId = correlationId,
            SchemaVersionKey = schemaVersionKey,
            Signature = signature,
            RawHeaders = (IReadOnlyDictionary<string, string>?)headerDictionary ?? EmptyHeaders
        };
    }

    private static string DecodeStrictUtf8(string headerName, byte[] valueBytes)
    {
        try
        {
            var decoded = StrictUtf8.GetString(valueBytes);
            if (ContainsProhibitedCharacters(decoded))
            {
                throw new InvalidDataException($"Kafka header '{headerName}' contains prohibited control characters.");
            }

            return decoded;
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException($"Kafka header '{headerName}' is not valid UTF-8.", exception);
        }
    }

    private static void EnsureMaximumSize(string headerName, byte[] valueBytes, int maximumByteCount)
    {
        if (valueBytes.Length is 0 || valueBytes.Length > maximumByteCount)
        {
            throw new InvalidDataException($"Kafka header '{headerName}' has an invalid size.");
        }
    }

    private static bool TryDecodeSafeIdentifier(byte[] valueBytes, int maximumByteCount, out string identifier)
    {
        identifier = string.Empty;
        if (valueBytes.Length is 0 || valueBytes.Length > maximumByteCount)
        {
            return false;
        }

        string candidate;
        try
        {
            candidate = StrictUtf8.GetString(valueBytes);
        }
        catch (DecoderFallbackException)
        {
            return false;
        }

        if (!IsSafeOpaqueValue(candidate))
        {
            return false;
        }

        identifier = candidate;
        return true;
    }

    private static bool IsSafeSchemaKey(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        foreach (var character in value)
        {
            if (!(char.IsAsciiLetterOrDigit(character) || character is '.' or '-' or '_' or ':'))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsSafeHeaderKey(string value)
    {
        foreach (var character in value)
        {
            if (!(char.IsAsciiLetterOrDigit(character) || character is '-' or '_' or '.'))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsSafeOpaqueValue(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        foreach (var character in value)
        {
            if (char.IsWhiteSpace(character))
            {
                return false;
            }
        }

        return !ContainsProhibitedCharacters(value);
    }

    private static bool ContainsProhibitedCharacters(string value)
    {
        foreach (var character in value)
        {
            var category = char.GetUnicodeCategory(character);
            if (category is UnicodeCategory.Control
                or UnicodeCategory.Format
                or UnicodeCategory.Surrogate
                or UnicodeCategory.PrivateUse
                or UnicodeCategory.OtherNotAssigned)
            {
                return true;
            }
        }

        return false;
    }
}
