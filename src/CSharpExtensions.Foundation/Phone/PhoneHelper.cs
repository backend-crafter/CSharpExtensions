using PhoneNumbers;

namespace CSharpExtensions.Foundation.Phone;

/// <summary>
/// Bounded phone-number validation and normalization based on libphonenumber metadata.
/// </summary>
public static class PhoneHelper
{
    private const int MaximumInputLength = 128;
    private const int MaximumDigits = 32;
    private static readonly PhoneNumberUtil PhoneUtil = PhoneNumberUtil.GetInstance();

    /// <summary>
    /// Normalizes an input phone number to E.164 when valid. For compatibility, a bounded digits-only
    /// representation is returned when libphonenumber cannot validate the value.
    /// </summary>
    public static string? NormalizePhoneNumber(string? phone)
    {
        if (string.IsNullOrEmpty(phone))
        {
            return phone;
        }

        if (!TryClean(phone, out var cleaned))
        {
            return null;
        }

        try
        {
            var parsed = PhoneUtil.Parse(cleaned, null);
            return PhoneUtil.IsValidNumber(parsed)
                ? PhoneUtil.Format(parsed, PhoneNumberFormat.E164)
                : cleaned;
        }
        catch (NumberParseException)
        {
            return cleaned;
        }
    }

    /// <summary>
    /// Validates a bounded phone number after ASCII-digit normalization.
    /// </summary>
    public static bool IsValidPhoneNumber(string? phone)
    {
        if (string.IsNullOrEmpty(phone) || !TryClean(phone, out var cleaned))
        {
            return false;
        }

        try
        {
            var parsed = PhoneUtil.Parse(cleaned, null);
            return PhoneUtil.IsValidNumber(parsed);
        }
        catch (NumberParseException)
        {
            return false;
        }
    }

    private static bool TryClean(string phone, out string cleaned)
    {
        cleaned = string.Empty;
        if (phone.Length > MaximumInputLength)
        {
            return false;
        }

        Span<char> buffer = stackalloc char[MaximumDigits + 1];
        buffer[0] = '+';
        var length = 1;

        try
        {
            foreach (var character in phone)
            {
                if (!char.IsAsciiDigit(character))
                {
                    continue;
                }

                if (length > MaximumDigits)
                {
                    return false;
                }

                buffer[length++] = character;
            }

            if (length == 1)
            {
                return false;
            }

            cleaned = new string(buffer[..length]);
            return true;
        }
        finally
        {
            buffer.Clear();
        }
    }
}
