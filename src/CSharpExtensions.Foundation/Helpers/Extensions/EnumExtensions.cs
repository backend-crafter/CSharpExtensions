namespace CSharpExtensions.Foundation.Helpers.Extensions;

/// <summary>
/// High-performance extensions for working with Enums.
/// </summary>
public static class EnumExtensions
{
    /// <summary>
    /// Gets the name of the enum value.
    /// </summary>
    public static string GetName<TEnum>(this TEnum value) where TEnum : struct, Enum
    {
        return Enum.GetName(value) ?? value.ToString();
    }

    /// <summary>
    /// Parses a string to an enum value.
    /// </summary>
    public static TEnum ToEnum<TEnum>(this string value, bool ignoreCase = true) where TEnum : struct, Enum
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        if (!TryToEnum(value, out TEnum result, ignoreCase))
        {
            throw new ArgumentException($"'{value}' is not a defined {typeof(TEnum).Name} value.", nameof(value));
        }

        return result;
    }

    /// <summary>
    /// Tries to parse a string to an enum value.
    /// </summary>
    public static TEnum? ToNullableEnum<TEnum>(this string? value, bool ignoreCase = true) where TEnum : struct, Enum
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        return TryToEnum(value, out TEnum result, ignoreCase) ? result : null;
    }

    /// <summary>
    /// Tries to parse a defined enum value. Flags combinations are accepted only
    /// when every bit belongs to a declared flag.
    /// </summary>
    public static bool TryToEnum<TEnum>(
        this string? value,
        out TEnum result,
        bool ignoreCase = true)
        where TEnum : struct, Enum
    {
        result = default;
        return !string.IsNullOrWhiteSpace(value) &&
               Enum.TryParse(value, ignoreCase, out result) &&
               IsDefined(result);
    }

    private static bool IsDefined<TEnum>(TEnum value) where TEnum : struct, Enum
    {
        var enumType = typeof(TEnum);
        if (Enum.IsDefined(enumType, value))
        {
            return true;
        }

        if (!enumType.IsDefined(typeof(FlagsAttribute), inherit: false))
        {
            return false;
        }

        var valueBits = ToUInt64(value);
        ulong allowedBits = 0;
        foreach (var declaredValue in Enum.GetValues<TEnum>())
        {
            allowedBits |= ToUInt64(declaredValue);
        }

        return valueBits != 0 && (valueBits & ~allowedBits) == 0;
    }

    private static ulong ToUInt64<TEnum>(TEnum value) where TEnum : struct, Enum
    {
        return Type.GetTypeCode(Enum.GetUnderlyingType(typeof(TEnum))) switch
        {
            TypeCode.SByte => unchecked((ulong)Convert.ToSByte(value)),
            TypeCode.Int16 => unchecked((ulong)Convert.ToInt16(value)),
            TypeCode.Int32 => unchecked((ulong)Convert.ToInt32(value)),
            TypeCode.Int64 => unchecked((ulong)Convert.ToInt64(value)),
            TypeCode.Byte => Convert.ToByte(value),
            TypeCode.UInt16 => Convert.ToUInt16(value),
            TypeCode.UInt32 => Convert.ToUInt32(value),
            TypeCode.UInt64 => Convert.ToUInt64(value),
            _ => throw new InvalidOperationException("Unsupported enum underlying type.")
        };
    }
}
