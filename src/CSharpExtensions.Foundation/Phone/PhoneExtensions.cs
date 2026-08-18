namespace CSharpExtensions.Foundation.Phone;

/// <summary>
/// Fluent string extension methods for phone numbers validation and normalization.
/// </summary>
public static class PhoneExtensions
{
    /// <summary>
    /// Extension method to normalize a phone number using E.164 standard format.
    /// </summary>
    public static string? NormalizePhone(this string? phone)
    {
        return PhoneHelper.NormalizePhoneNumber(phone);
    }

    /// <summary>
    /// Extension method to validate if the phone number structure matches a valid global phone number plan.
    /// </summary>
    public static bool IsValidPhone(this string? phone)
    {
        return PhoneHelper.IsValidPhoneNumber(phone);
    }
}
