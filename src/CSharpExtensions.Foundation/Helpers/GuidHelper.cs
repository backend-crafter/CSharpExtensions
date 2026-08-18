using System.Security.Cryptography;

namespace CSharpExtensions.Foundation.Helpers;

/// <summary>
/// Creates and inspects RFC 9562 UUID version 7 identifiers.
/// </summary>
/// <remarks>
/// UUID version 7 embeds the Unix timestamp in milliseconds in the first 48 RFC-order bits.
/// The embedded value is the identifier generation time, not an authoritative database commit time.
/// SQL Server <c>uniqueidentifier</c> uses a different comparison order, so UUID version 7 alone
/// does not guarantee clustered-index locality in SQL Server.
/// </remarks>
public static class GuidHelper
{
    internal const long MaxUnixTimestampMilliseconds = 0xFFFFFFFFFFFF;
    private const ushort MaxSequence = 0x0FFF;
    private const ushort MaxInitialSequence = 0x07FF;

    private static readonly Uuid7MonotonicState MonotonicState = new();

    /// <summary>
    /// Creates an RFC 9562 UUID version 7 using the current UTC time.
    /// </summary>
    /// <remarks>
    /// Values are allocated monotonically in RFC byte/string order by the process-wide state.
    /// Concurrent callers may complete in a different order. The 12-bit <c>rand_a</c> field is
    /// used as a sequence with a randomized 11-bit initial value for each new timestamp. At least
    /// 2048 identifiers can be allocated before sequence overflow advances the logical timestamp
    /// by one millisecond. During clock rollback or sequence overflow, the embedded logical
    /// timestamp may temporarily be ahead of wall-clock time. Monotonicity is not coordinated
    /// across processes or hosts.
    /// </remarks>
    /// <returns>A new UUID version 7.</returns>
    public static Guid CreateVersion7()
    {
        var state = MonotonicState.Next(TimeProvider.System.GetUtcNow().ToUnixTimeMilliseconds());
        return CreateMonotonicVersion7(state.TimestampMilliseconds, state.Sequence);
    }

    /// <summary>
    /// Creates an RFC 9562 UUID version 7 containing the supplied timestamp.
    /// </summary>
    /// <remarks>
    /// This overload is intended for controlled migrations and imports. It uses random data for
    /// all non-timestamp bits and does not participate in the process-wide monotonic sequence.
    /// Multiple values created with the same timestamp therefore have no defined relative order.
    /// </remarks>
    /// <param name="timestamp">The UTC instant to encode with millisecond precision.</param>
    /// <returns>A new UUID version 7 containing <paramref name="timestamp"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The timestamp is before the Unix epoch or does not fit the UUID version 7 timestamp field.
    /// </exception>
    public static Guid CreateVersion7(DateTimeOffset timestamp)
    {
        var timestampMilliseconds = ValidateTimestamp(timestamp.ToUnixTimeMilliseconds(), nameof(timestamp));

        Span<byte> rfcBytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(rfcBytes[6..]);
        WriteTimestamp(rfcBytes, timestampMilliseconds);
        rfcBytes[6] = (byte)((rfcBytes[6] & 0x0F) | 0x70);
        rfcBytes[8] = (byte)((rfcBytes[8] & 0x3F) | 0x80);

        return CreateGuidFromRfcBytes(rfcBytes);
    }

    /// <summary>
    /// Gets the Unix timestamp embedded in an RFC 9562 UUID version 7.
    /// </summary>
    /// <param name="value">The UUID version 7 value.</param>
    /// <returns>The embedded timestamp with millisecond precision.</returns>
    /// <exception cref="ArgumentException">
    /// The value is not an RFC 9562 UUID version 7 or its timestamp is outside
    /// the range supported by <see cref="DateTimeOffset"/>.
    /// </exception>
    public static DateTimeOffset GetVersion7Timestamp(Guid value)
    {
        if (!TryGetVersion7Timestamp(value, out var timestamp))
        {
            throw new ArgumentException(
                "The value is not a valid RFC 9562 UUID version 7 with a supported timestamp.",
                nameof(value));
        }

        return timestamp;
    }

    /// <summary>
    /// Attempts to get the Unix timestamp embedded in an RFC 9562 UUID version 7.
    /// </summary>
    /// <param name="value">The value to inspect.</param>
    /// <param name="timestamp">
    /// The embedded timestamp with millisecond precision when this method returns <see langword="true"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="value"/> is an RFC 9562 UUID version 7
    /// with a timestamp supported by <see cref="DateTimeOffset"/>; otherwise, <see langword="false"/>.
    /// </returns>
    public static bool TryGetVersion7Timestamp(Guid value, out DateTimeOffset timestamp)
    {
        Span<byte> rfcBytes = stackalloc byte[16];
        WriteRfcBytes(value, rfcBytes);

        var hasVersion7 = (rfcBytes[6] >> 4) == 7;
        var hasRfcVariant = (rfcBytes[8] & 0xC0) == 0x80;
        if (!hasVersion7 || !hasRfcVariant)
        {
            timestamp = default;
            return false;
        }

        var timestampMilliseconds = ReadTimestamp(rfcBytes);
        if (timestampMilliseconds > DateTimeOffset.MaxValue.ToUnixTimeMilliseconds())
        {
            timestamp = default;
            return false;
        }

        timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timestampMilliseconds);
        return true;
    }

    private static Guid CreateMonotonicVersion7(long timestampMilliseconds, ushort sequence)
    {
        Span<byte> rfcBytes = stackalloc byte[16];
        WriteTimestamp(rfcBytes, timestampMilliseconds);

        rfcBytes[6] = (byte)(0x70 | (sequence >> 8));
        rfcBytes[7] = (byte)sequence;

        RandomNumberGenerator.Fill(rfcBytes[8..]);
        rfcBytes[8] = (byte)((rfcBytes[8] & 0x3F) | 0x80);

        return CreateGuidFromRfcBytes(rfcBytes);
    }

    private static void WriteTimestamp(Span<byte> rfcBytes, long timestampMilliseconds)
    {
        rfcBytes[0] = (byte)(timestampMilliseconds >> 40);
        rfcBytes[1] = (byte)(timestampMilliseconds >> 32);
        rfcBytes[2] = (byte)(timestampMilliseconds >> 24);
        rfcBytes[3] = (byte)(timestampMilliseconds >> 16);
        rfcBytes[4] = (byte)(timestampMilliseconds >> 8);
        rfcBytes[5] = (byte)timestampMilliseconds;
    }

    private static long ReadTimestamp(ReadOnlySpan<byte> rfcBytes)
    {
        return ((long)rfcBytes[0] << 40)
             | ((long)rfcBytes[1] << 32)
             | ((long)rfcBytes[2] << 24)
             | ((long)rfcBytes[3] << 16)
             | ((long)rfcBytes[4] << 8)
             | rfcBytes[5];
    }

    private static long ValidateTimestamp(long timestampMilliseconds, string parameterName)
    {
        if (timestampMilliseconds is < 0 or > MaxUnixTimestampMilliseconds)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                "The timestamp must be within the unsigned 48-bit UUID version 7 range.");
        }

        return timestampMilliseconds;
    }

    private static ushort CreateSequenceSeed()
    {
        return (ushort)RandomNumberGenerator.GetInt32(MaxInitialSequence + 1);
    }

    private static Guid CreateGuidFromRfcBytes(Span<byte> rfcBytes)
    {
        SwapGuidEndianFields(rfcBytes);
        return new Guid(rfcBytes);
    }

    private static void WriteRfcBytes(Guid value, Span<byte> destination)
    {
        if (!value.TryWriteBytes(destination))
        {
            throw new ArgumentException("The destination must contain at least 16 bytes.", nameof(destination));
        }

        SwapGuidEndianFields(destination);
    }

    private static void SwapGuidEndianFields(Span<byte> bytes)
    {
        Swap(bytes, 0, 3);
        Swap(bytes, 1, 2);
        Swap(bytes, 4, 5);
        Swap(bytes, 6, 7);
    }

    private static void Swap(Span<byte> bytes, int i, int j)
    {
        (bytes[i], bytes[j]) = (bytes[j], bytes[i]);
    }

    internal readonly record struct Uuid7TimestampSequence(long TimestampMilliseconds, ushort Sequence);

    internal sealed class Uuid7MonotonicState
    {
        private readonly Func<ushort> _sequenceSeedFactory;
        private long _packedState = -1;

        internal Uuid7MonotonicState()
            : this(CreateSequenceSeed)
        {
        }

        internal Uuid7MonotonicState(Func<ushort> sequenceSeedFactory)
        {
            _sequenceSeedFactory = sequenceSeedFactory
                ?? throw new ArgumentNullException(nameof(sequenceSeedFactory));
        }

        internal Uuid7TimestampSequence Next(long currentTimestampMilliseconds)
        {
            ValidateTimestamp(currentTimestampMilliseconds, nameof(currentTimestampMilliseconds));

            while (true)
            {
                var previous = Volatile.Read(ref _packedState);
                long nextTimestamp;
                ushort nextSequence;

                if (previous < 0)
                {
                    nextTimestamp = currentTimestampMilliseconds;
                    nextSequence = GetSequenceSeed();
                }
                else
                {
                    var previousTimestamp = previous >> 12;
                    var previousSequence = (ushort)(previous & MaxSequence);

                    if (currentTimestampMilliseconds > previousTimestamp)
                    {
                        nextTimestamp = currentTimestampMilliseconds;
                        nextSequence = GetSequenceSeed();
                    }
                    else if (previousSequence < MaxSequence)
                    {
                        nextTimestamp = previousTimestamp;
                        nextSequence = (ushort)(previousSequence + 1);
                    }
                    else
                    {
                        if (previousTimestamp == MaxUnixTimestampMilliseconds)
                        {
                            throw new InvalidOperationException("The UUID version 7 timestamp range is exhausted.");
                        }

                        nextTimestamp = previousTimestamp + 1;
                        nextSequence = GetSequenceSeed();
                    }
                }

                var next = (nextTimestamp << 12) | nextSequence;
                if (Interlocked.CompareExchange(ref _packedState, next, previous) == previous)
                {
                    return new Uuid7TimestampSequence(nextTimestamp, nextSequence);
                }
            }
        }

        private ushort GetSequenceSeed()
        {
            var sequence = _sequenceSeedFactory();
            if (sequence > MaxInitialSequence)
            {
                throw new InvalidOperationException(
                    $"The UUID version 7 sequence seed must be between 0 and {MaxInitialSequence}.");
            }

            return sequence;
        }
    }
}
