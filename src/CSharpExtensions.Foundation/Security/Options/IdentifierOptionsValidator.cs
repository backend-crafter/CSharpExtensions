using Microsoft.Extensions.Options;

namespace CSharpExtensions.Foundation.Security.Options;

/// <summary>
/// Validates bounded Sqids identifier configuration.
/// </summary>
public sealed class IdentifierOptionsValidator : IValidateOptions<IdentifierOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, IdentifierOptions options)
    {
        var failures = new List<string>();

        if (options.MinLength is < 0 or > 64)
        {
            failures.Add("IdentifierOptions.MinLength must be between 0 and 64.");
        }

        if (string.IsNullOrEmpty(options.Alphabet) ||
            options.Alphabet.Length is < 3 or > 255)
        {
            failures.Add("IdentifierOptions.Alphabet must contain between 3 and 255 characters.");
        }
        else if (options.Alphabet.Distinct().Count() != options.Alphabet.Length)
        {
            failures.Add("IdentifierOptions.Alphabet must contain unique characters.");
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}
