using System.Buffers;
using System.ComponentModel;
using System.Security.Cryptography;
using System.Text;

namespace CSharpExtensions.Core.Security.Extensions;

/// <summary>
/// Provides bounded hashing and HMAC helpers for UTF-8 text and binary messages.
/// </summary>
public static class HashGenerator
{
    private const int Utf8BufferSize = 4096;
    private const int MaximumHmacKeyBytes = 64 * 1024;
    private const int MinimumStrongHmacKeyBytes = 32;

    private static ReadOnlySpan<byte> HexLookup => "0123456789abcdef"u8;

    /// <summary>
    /// Computes a SHA-256 hash of a UTF-8 string and returns lowercase hexadecimal text.
    /// </summary>
    public static string ComputeHash(string input)
    {
        if (string.IsNullOrEmpty(input))
        {
            return string.Empty;
        }

        Span<byte> hash = stackalloc byte[32];
        ComputeUtf8Hash(input, HashAlgorithmName.SHA256, hash);

        try
        {
            return ToLowerHex(hash);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(hash);
        }
    }

    /// <summary>
    /// Computes an HMAC-SHA-256 signature of a binary message.
    /// </summary>
    public static string ComputeHmac(string key, ReadOnlySpan<byte> messageBytes)
    {
        return ComputeHmacCore(key, messageBytes, HashAlgorithmName.SHA256);
    }

    /// <summary>
    /// Computes an HMAC-SHA-256 signature of a UTF-8 message.
    /// </summary>
    public static string ComputeHmac(string key, string message)
    {
        if (string.IsNullOrEmpty(message))
        {
            return string.Empty;
        }

        return ComputeUtf8HmacCore(key, message, HashAlgorithmName.SHA256);
    }

    /// <summary>
    /// Computes an HMAC signature with SHA-256, SHA-384, or SHA-512.
    /// </summary>
    public static string ComputeStrongHmac(
        string key,
        ReadOnlySpan<byte> messageBytes,
        HashAlgorithmName hashAlgorithm)
    {
        EnsureStrongAlgorithm(hashAlgorithm);
        EnsureStrongKey(key);
        return ComputeHmacCore(key, messageBytes, hashAlgorithm);
    }

    /// <summary>
    /// Computes an HMAC signature over UTF-8 text with SHA-256, SHA-384, or SHA-512.
    /// </summary>
    public static string ComputeStrongHmac(
        string key,
        string message,
        HashAlgorithmName hashAlgorithm)
    {
        ArgumentNullException.ThrowIfNull(message);
        EnsureStrongAlgorithm(hashAlgorithm);
        EnsureStrongKey(key);
        return ComputeUtf8HmacCore(key, message, hashAlgorithm);
    }

    /// <summary>
    /// Verifies a lowercase or uppercase hexadecimal strong HMAC in constant time.
    /// </summary>
    public static bool VerifyStrongHmac(
        string key,
        string message,
        string? expectedHex,
        HashAlgorithmName hashAlgorithm)
    {
        ArgumentNullException.ThrowIfNull(message);
        EnsureStrongAlgorithm(hashAlgorithm);
        EnsureStrongKey(key);

        var hashSize = GetHashByteLength(hashAlgorithm);
        if (expectedHex is null || expectedHex.Length != hashSize * 2)
        {
            return false;
        }

        var keyBytes = EncodeKey(key);
        Span<byte> actualHash = stackalloc byte[hashSize];
        Span<byte> expectedHash = stackalloc byte[hashSize];

        try
        {
            if (!TryDecodeHex(expectedHex, expectedHash))
            {
                return false;
            }

            using var incrementalHash = IncrementalHash.CreateHMAC(hashAlgorithm, keyBytes);
            AppendUtf8(incrementalHash, message);
            if (!incrementalHash.TryGetHashAndReset(actualHash, out var bytesWritten) ||
                bytesWritten != hashSize)
            {
                throw new CryptographicException("Failed to compute the HMAC result.");
            }

            return CryptographicOperations.FixedTimeEquals(actualHash, expectedHash);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(keyBytes);
            CryptographicOperations.ZeroMemory(actualHash);
            CryptographicOperations.ZeroMemory(expectedHash);
        }
    }

    /// <summary>
    /// Computes an explicitly requested legacy HMAC with SHA-1 or MD5.
    /// </summary>
    /// <remarks>
    /// This API exists only for verification of protocols that still mandate a weak algorithm.
    /// Do not use it for new signatures or authentication protocols.
    /// </remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static string ComputeLegacyHmac(
        string key,
        ReadOnlySpan<byte> messageBytes,
        HashAlgorithmName hashAlgorithm)
    {
        EnsureLegacyAlgorithm(hashAlgorithm);
        return ComputeHmacCore(key, messageBytes, hashAlgorithm);
    }

    /// <summary>
    /// Computes an explicitly requested legacy HMAC over UTF-8 text with SHA-1 or MD5.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static string ComputeLegacyHmac(
        string key,
        string message,
        HashAlgorithmName hashAlgorithm)
    {
        if (string.IsNullOrEmpty(message))
        {
            return string.Empty;
        }

        EnsureLegacyAlgorithm(hashAlgorithm);
        return ComputeUtf8HmacCore(key, message, hashAlgorithm);
    }

    /// <summary>
    /// Compatibility overload for callers that selected the algorithm dynamically.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static string ComputeHmac(
        string key,
        ReadOnlySpan<byte> messageBytes,
        HashAlgorithmName hashAlgorithm = default)
    {
        var algorithm = NormalizeAlgorithm(hashAlgorithm);
        return ComputeHmacCore(key, messageBytes, algorithm);
    }

    /// <summary>
    /// Compatibility overload for callers that selected the algorithm dynamically.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static string ComputeHmac(
        string key,
        string message,
        HashAlgorithmName hashAlgorithm = default)
    {
        if (string.IsNullOrEmpty(message))
        {
            return string.Empty;
        }

        var algorithm = NormalizeAlgorithm(hashAlgorithm);
        return ComputeUtf8HmacCore(key, message, algorithm);
    }

    /// <summary>
    /// Computes a SHA-256 hash of UTF-8 text and returns Base64URL without padding.
    /// </summary>
    public static string ComputeSha256Base64Url(string input)
    {
        if (string.IsNullOrEmpty(input))
        {
            return string.Empty;
        }

        Span<byte> hash = stackalloc byte[32];
        Span<char> base64 = stackalloc char[44];
        ComputeUtf8Hash(input, HashAlgorithmName.SHA256, hash);

        try
        {
            if (!Convert.TryToBase64Chars(hash, base64, out var charsWritten))
            {
                throw new CryptographicException("Failed to encode the SHA-256 result.");
            }

            var resultLength = charsWritten;
            while (resultLength > 0 && base64[resultLength - 1] == '=')
            {
                resultLength--;
            }

            for (var index = 0; index < resultLength; index++)
            {
                base64[index] = base64[index] switch
                {
                    '+' => '-',
                    '/' => '_',
                    _ => base64[index]
                };
            }

            return new string(base64[..resultLength]);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(hash);
            base64.Clear();
        }
    }

    private static string ComputeHmacCore(
        string key,
        ReadOnlySpan<byte> messageBytes,
        HashAlgorithmName hashAlgorithm)
    {
        var keyBytes = EncodeKey(key);
        var hashSize = GetHashByteLength(hashAlgorithm);
        Span<byte> hash = stackalloc byte[hashSize];

        try
        {
            ComputeHmacHashData(hashAlgorithm, keyBytes, messageBytes, hash);
            return ToLowerHex(hash);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(keyBytes);
            CryptographicOperations.ZeroMemory(hash);
        }
    }

    private static string ComputeUtf8HmacCore(
        string key,
        string message,
        HashAlgorithmName hashAlgorithm)
    {
        var keyBytes = EncodeKey(key);
        var hashSize = GetHashByteLength(hashAlgorithm);
        Span<byte> hash = stackalloc byte[hashSize];

        try
        {
            using var incrementalHash = IncrementalHash.CreateHMAC(hashAlgorithm, keyBytes);
            AppendUtf8(incrementalHash, message);

            if (!incrementalHash.TryGetHashAndReset(hash, out var written) || written != hashSize)
            {
                throw new CryptographicException("Failed to compute the HMAC result.");
            }

            return ToLowerHex(hash);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(keyBytes);
            CryptographicOperations.ZeroMemory(hash);
        }
    }

    private static void ComputeUtf8Hash(
        string input,
        HashAlgorithmName hashAlgorithm,
        Span<byte> destination)
    {
        using var incrementalHash = IncrementalHash.CreateHash(hashAlgorithm);
        AppendUtf8(incrementalHash, input);

        if (!incrementalHash.TryGetHashAndReset(destination, out var written) ||
            written != destination.Length)
        {
            throw new CryptographicException("Failed to compute the hash result.");
        }
    }

    private static void AppendUtf8(IncrementalHash hash, string input)
    {
        var rentedBuffer = ArrayPool<byte>.Shared.Rent(Utf8BufferSize);
        var encoder = Encoding.UTF8.GetEncoder();
        var remaining = input.AsSpan();

        try
        {
            while (!remaining.IsEmpty)
            {
                encoder.Convert(
                    remaining,
                    rentedBuffer.AsSpan(0, Utf8BufferSize),
                    flush: true,
                    out var charsUsed,
                    out var bytesUsed,
                    out _);

                if (charsUsed == 0 && bytesUsed == 0)
                {
                    throw new EncoderFallbackException("UTF-8 encoding did not make progress.");
                }

                hash.AppendData(rentedBuffer.AsSpan(0, bytesUsed));
                CryptographicOperations.ZeroMemory(rentedBuffer.AsSpan(0, bytesUsed));
                remaining = remaining[charsUsed..];
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(rentedBuffer);
            ArrayPool<byte>.Shared.Return(rentedBuffer, clearArray: true);
        }
    }

    private static byte[] EncodeKey(string key)
    {
        ArgumentNullException.ThrowIfNull(key);

        var byteCount = Encoding.UTF8.GetByteCount(key);
        if (byteCount > MaximumHmacKeyBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(key),
                $"The UTF-8 HMAC key must not exceed {MaximumHmacKeyBytes} bytes.");
        }

        return Encoding.UTF8.GetBytes(key);
    }

    private static string ToLowerHex(ReadOnlySpan<byte> hash)
    {
        Span<char> chars = stackalloc char[hash.Length * 2];
        try
        {
            for (var index = 0; index < hash.Length; index++)
            {
                var value = hash[index];
                chars[index * 2] = (char)HexLookup[value >> 4];
                chars[(index * 2) + 1] = (char)HexLookup[value & 0x0F];
            }

            return new string(chars);
        }
        finally
        {
            chars.Clear();
        }
    }

    private static bool TryDecodeHex(ReadOnlySpan<char> source, Span<byte> destination)
    {
        if (source.Length != destination.Length * 2)
        {
            return false;
        }

        for (var index = 0; index < destination.Length; index++)
        {
            var high = DecodeHexNibble(source[index * 2]);
            var low = DecodeHexNibble(source[(index * 2) + 1]);
            if (high < 0 || low < 0)
            {
                return false;
            }

            destination[index] = (byte)((high << 4) | low);
        }

        return true;
    }

    private static int DecodeHexNibble(char value)
    {
        if (value is >= '0' and <= '9') return value - '0';
        if (value is >= 'a' and <= 'f') return value - 'a' + 10;
        if (value is >= 'A' and <= 'F') return value - 'A' + 10;
        return -1;
    }

    private static HashAlgorithmName NormalizeAlgorithm(HashAlgorithmName hashAlgorithm)
    {
        return string.IsNullOrEmpty(hashAlgorithm.Name)
            ? HashAlgorithmName.SHA256
            : hashAlgorithm;
    }

    private static void EnsureStrongAlgorithm(HashAlgorithmName hashAlgorithm)
    {
        if (hashAlgorithm != HashAlgorithmName.SHA256 &&
            hashAlgorithm != HashAlgorithmName.SHA384 &&
            hashAlgorithm != HashAlgorithmName.SHA512)
        {
            throw new NotSupportedException(
                $"Hash algorithm '{hashAlgorithm.Name}' is not an approved strong HMAC algorithm.");
        }
    }

    private static void EnsureStrongKey(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var keyByteCount = Encoding.UTF8.GetByteCount(key);
        if (keyByteCount < MinimumStrongHmacKeyBytes)
        {
            throw new ArgumentException(
                $"Strong HMAC keys must contain at least {MinimumStrongHmacKeyBytes} UTF-8 bytes.",
                nameof(key));
        }

        if (keyByteCount > MaximumHmacKeyBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(key),
                $"The UTF-8 HMAC key must not exceed {MaximumHmacKeyBytes} bytes.");
        }
    }

    private static void EnsureLegacyAlgorithm(HashAlgorithmName hashAlgorithm)
    {
        if (hashAlgorithm != HashAlgorithmName.SHA1 && hashAlgorithm != HashAlgorithmName.MD5)
        {
            throw new ArgumentException(
                "The legacy API accepts only SHA-1 or MD5.",
                nameof(hashAlgorithm));
        }
    }

    private static int GetHashByteLength(HashAlgorithmName name)
    {
        if (name == HashAlgorithmName.SHA256) return 32;
        if (name == HashAlgorithmName.SHA512) return 64;
        if (name == HashAlgorithmName.SHA384) return 48;
        if (name == HashAlgorithmName.SHA1) return 20;
        if (name == HashAlgorithmName.MD5) return 16;
        throw new NotSupportedException($"Hash algorithm '{name.Name}' is not supported for HMAC.");
    }

    private static void ComputeHmacHashData(
        HashAlgorithmName name,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> source,
        Span<byte> destination)
    {
        if (name == HashAlgorithmName.SHA256)
        {
            HMACSHA256.HashData(key, source, destination);
            return;
        }

        if (name == HashAlgorithmName.SHA512)
        {
            HMACSHA512.HashData(key, source, destination);
            return;
        }

        if (name == HashAlgorithmName.SHA384)
        {
            HMACSHA384.HashData(key, source, destination);
            return;
        }

        if (name == HashAlgorithmName.SHA1)
        {
            HMACSHA1.HashData(key, source, destination);
            return;
        }

        if (name == HashAlgorithmName.MD5)
        {
            HMACMD5.HashData(key, source, destination);
            return;
        }

        throw new NotSupportedException($"Hash algorithm '{name.Name}' is not supported for HMAC.");
    }
}
