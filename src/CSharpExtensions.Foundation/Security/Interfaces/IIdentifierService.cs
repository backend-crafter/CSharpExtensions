namespace CSharpExtensions.Foundation.Security.Interfaces;

/// <summary>
/// Service for generating short, unique, and readable strings from long IDs.
/// </summary>
public interface IIdentifierService
{
    /// <summary>
    /// Encodes a long ID into a short string.
    /// </summary>
    /// <param name="identifier">The long ID to encode.</param>
    /// <returns>A short, obfuscated string.</returns>
    string Encode(long identifier);

    /// <summary>
    /// Decodes a short string back into its original long ID.
    /// </summary>
    /// <param name="shortIdentifier">The short string to decode.</param>
    /// <returns>The original long ID, or null if decoding fails.</returns>
    long? Decode(string shortIdentifier);
}
