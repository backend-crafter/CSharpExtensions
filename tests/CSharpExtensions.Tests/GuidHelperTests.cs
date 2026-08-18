using CSharpExtensions.Foundation.Helpers;
using Xunit;

namespace CSharpExtensions.Tests;

public sealed class GuidHelperTests
{
    [Fact]
    public void CreateVersion7_ShouldEncodeAndExtractSuppliedTimestamp()
    {
        const long unixTimestampMilliseconds = 0x017F22E279B0;
        var expected = DateTimeOffset.FromUnixTimeMilliseconds(unixTimestampMilliseconds);

        var value = GuidHelper.CreateVersion7(expected);

        Assert.StartsWith("017f22e279b0", value.ToString("N"));
        Assert.Equal('7', value.ToString("N")[12]);
        Assert.Contains(value.ToString("N")[16], "89ab");
        Assert.True(GuidHelper.TryGetVersion7Timestamp(value, out var extracted));
        Assert.Equal(expected, extracted);
        Assert.Equal(expected, GuidHelper.GetVersion7Timestamp(value));
    }

    [Fact]
    public void GetVersion7Timestamp_ShouldReadRfc9562Example()
    {
        var value = Guid.Parse("017f22e2-79b0-7cc3-98c4-dc0c0c07398f");
        var expected = DateTimeOffset.FromUnixTimeMilliseconds(0x017F22E279B0);

        Assert.Equal(expected, GuidHelper.GetVersion7Timestamp(value));
    }

    [Fact]
    public void CreateVersion7_ShouldSupportMaximumDateTimeOffsetTimestamp()
    {
        var expected = DateTimeOffset.FromUnixTimeMilliseconds(
            DateTimeOffset.MaxValue.ToUnixTimeMilliseconds());

        var value = GuidHelper.CreateVersion7(DateTimeOffset.MaxValue);

        Assert.Equal(expected, GuidHelper.GetVersion7Timestamp(value));
    }

    [Fact]
    public void CreateVersion7_ShouldRejectTimestampBeforeUnixEpoch()
    {
        var timestamp = DateTimeOffset.UnixEpoch.AddMilliseconds(-1);

        var exception = Assert.Throws<ArgumentOutOfRangeException>(
            () => GuidHelper.CreateVersion7(timestamp));

        Assert.Equal("timestamp", exception.ParamName);
    }

    [Fact]
    public void TryGetVersion7Timestamp_ShouldRejectOtherUuidVersions()
    {
        var result = GuidHelper.TryGetVersion7Timestamp(Guid.NewGuid(), out var timestamp);

        Assert.False(result);
        Assert.Equal(default, timestamp);
        Assert.Throws<ArgumentException>(() => GuidHelper.GetVersion7Timestamp(Guid.NewGuid()));
    }

    [Fact]
    public void TryGetVersion7Timestamp_ShouldRejectNonRfcVariant()
    {
        var value = Guid.Parse("017f22e2-79b0-7cc3-18c4-dc0c0c07398f");

        var result = GuidHelper.TryGetVersion7Timestamp(value, out var timestamp);

        Assert.False(result);
        Assert.Equal(default, timestamp);
    }

    [Fact]
    public void TryGetVersion7Timestamp_ShouldRejectTimestampOutsideDateTimeOffsetRange()
    {
        var value = Guid.Parse("ffffffff-ffff-7cc3-98c4-dc0c0c07398f");

        var result = GuidHelper.TryGetVersion7Timestamp(value, out var timestamp);

        Assert.False(result);
        Assert.Equal(default, timestamp);
    }

    [Fact]
    public void CreateVersion7_ShouldBeStrictlyIncreasingInRfcStringOrder()
    {
        var previous = GuidHelper.CreateVersion7().ToString("N");

        for (var i = 0; i < 10_000; i++)
        {
            var current = GuidHelper.CreateVersion7().ToString("N");
            Assert.True(string.CompareOrdinal(previous, current) < 0);
            previous = current;
        }
    }

    [Fact]
    public void MonotonicState_ShouldAdvanceTimestampAtEverySequenceOverflow()
    {
        const long timestampMilliseconds = 1_000;
        var state = new GuidHelper.Uuid7MonotonicState(static () => 0);
        GuidHelper.Uuid7TimestampSequence value = default;

        for (var i = 0; i <= 8_192; i++)
        {
            value = state.Next(timestampMilliseconds);
        }

        Assert.Equal(timestampMilliseconds + 2, value.TimestampMilliseconds);
        Assert.Equal(0, value.Sequence);
    }

    [Fact]
    public void MonotonicState_ShouldNotMoveBackWhenClockMovesBack()
    {
        var state = new GuidHelper.Uuid7MonotonicState(static () => 0);

        var first = state.Next(2_000);
        var second = state.Next(1_999);

        Assert.Equal(2_000, first.TimestampMilliseconds);
        Assert.Equal(0, first.Sequence);
        Assert.Equal(2_000, second.TimestampMilliseconds);
        Assert.Equal(1, second.Sequence);
    }

    [Fact]
    public void MonotonicState_ShouldUseConfiguredSeedForEachNewTimestamp()
    {
        const ushort seed = 1_234;
        var state = new GuidHelper.Uuid7MonotonicState(static () => seed);

        var first = state.Next(1_000);
        var second = state.Next(1_001);

        Assert.Equal(seed, first.Sequence);
        Assert.Equal(seed, second.Sequence);
    }

    [Fact]
    public void MonotonicState_ShouldRejectOverflowAtMaximumUuidTimestamp()
    {
        const ushort maximumInitialSeed = 0x07FF;
        var state = new GuidHelper.Uuid7MonotonicState(static () => maximumInitialSeed);

        for (var i = maximumInitialSeed; i <= 0x0FFF; i++)
        {
            state.Next(GuidHelper.MaxUnixTimestampMilliseconds);
        }

        Assert.Throws<InvalidOperationException>(
            () => state.Next(GuidHelper.MaxUnixTimestampMilliseconds));
    }

    [Fact]
    public void MonotonicState_ShouldReturnUniqueValuesUnderConcurrency()
    {
        const int count = 20_000;
        const long timestampMilliseconds = 10_000;
        var state = new GuidHelper.Uuid7MonotonicState(static () => 0);
        var values = new long[count];

        Parallel.For(0, count, index =>
        {
            var value = state.Next(timestampMilliseconds);
            values[index] = (value.TimestampMilliseconds << 12) | value.Sequence;
        });

        Assert.Equal(count, values.Distinct().Count());
        Assert.Equal((timestampMilliseconds << 12), values.Min());
        Assert.Equal((timestampMilliseconds << 12) + count - 1, values.Max());
    }
}
