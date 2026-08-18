namespace CSharpExtensions.Foundation.Security.Options;

/// <summary>
/// Configuration options for the identifier service.
/// </summary>
public class IdentifierOptions
{
    /// <summary>
    /// The default minimum length for the generated short IDs.
    /// </summary>
    public const int DefaultMinLength = 8;

    /// <summary>
    /// The default alphabet used for generating short IDs, excluding look-alike characters (0, O, 1, l, I).
    /// </summary>
    public const string DefaultAlphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";

    /// <summary>
    /// The minimum length of the resulting short ID.
    /// </summary>
    public int MinLength { get; set; } = DefaultMinLength;

    /// <summary>
    /// The custom alphabet to use for encoding.
    /// </summary>
    public string Alphabet { get; set; } = DefaultAlphabet;
}
