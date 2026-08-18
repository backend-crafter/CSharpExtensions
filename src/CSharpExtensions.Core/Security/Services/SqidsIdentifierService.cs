using CSharpExtensions.Core.Security.Interfaces;
using CSharpExtensions.Core.Security.Options;
using Microsoft.Extensions.Options;
using Sqids;

namespace CSharpExtensions.Core.Security.Services;

/// <summary>
/// Implementation of IIdentifierService using the Sqids algorithm.
/// </summary>
public sealed class SqidsIdentifierService : IIdentifierService
{
    private readonly SqidsEncoder<long> _encoder;

    /// <summary>
    /// Initializes a new instance of the SqidsIdentifierService with options.
    /// </summary>
    /// <param name="options">Configuration options for Sqids.</param>
    public SqidsIdentifierService(IOptions<IdentifierOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var identifierOptions = options.Value;
        var validation = new IdentifierOptionsValidator().Validate(null, identifierOptions);
        if (validation.Failed)
        {
            throw new OptionsValidationException(
                nameof(IdentifierOptions),
                typeof(IdentifierOptions),
                validation.Failures);
        }
        
        _encoder = new SqidsEncoder<long>(new SqidsOptions
        {
            Alphabet = identifierOptions.Alphabet,
            MinLength = identifierOptions.MinLength
        });
    }

    /// <inheritdoc />
    public string Encode(long identifier)
    {
        return _encoder.Encode(identifier);
    }

    /// <inheritdoc />
    public long? Decode(string shortIdentifier)
    {
        if (string.IsNullOrWhiteSpace(shortIdentifier) || shortIdentifier.Length > 256)
        {
            return null;
        }

        var result = _encoder.Decode(shortIdentifier);
        
        return result.Count == 1 ? result[0] : null;
    }
}
