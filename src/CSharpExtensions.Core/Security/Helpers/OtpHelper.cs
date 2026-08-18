using CSharpExtensions.Core.Security.Interfaces;
using CSharpExtensions.Core.Security.Options;
using CSharpExtensions.Core.Security.Services;

namespace CSharpExtensions.Core.Security.Helpers;

/// <summary>
/// Provides a static helper for generating cryptographically secure One-Time Passwords (OTPs).
/// Supports custom character sets, uppercase conversion, and confusable character avoidance.
/// Exposes standard test overrides to facilitate deterministic unit testing of OTP dispatches.
/// </summary>
public static class OtpHelper
{
    private static readonly IOtpGenerator DefaultGenerator = new SecureOtpGenerator();
    private static readonly AsyncLocal<IOtpGenerator?> ScopedGenerator = new();

    /// <summary>
    /// Overrides the default secure OTP generator with a custom generator.
    /// Primarily used in unit tests to inject mock OTP generators.
    /// </summary>
    /// <param name="generator">The custom OTP generator to use.</param>
    public static void SetGenerator(IOtpGenerator generator)
    {
        ScopedGenerator.Value = generator ?? throw new ArgumentNullException(nameof(generator));
    }

    /// <summary>
    /// Resets the generator to the default cryptographically secure implementation.
    /// </summary>
    public static void Reset()
    {
        ScopedGenerator.Value = null;
    }

    /// <summary>
    /// Applies an OTP generator override to the current execution context and restores the previous value on disposal.
    /// </summary>
    /// <remarks>This compatibility API is intended only for tests. Production code should inject <see cref="IOtpGenerator"/>.</remarks>
    public static IDisposable BeginOverride(IOtpGenerator generator)
    {
        ArgumentNullException.ThrowIfNull(generator);
        var previous = ScopedGenerator.Value;
        ScopedGenerator.Value = generator;
        return new GeneratorOverrideScope(previous);
    }

    /// <summary>
    /// Generates a cryptographically secure One-Time Password based on the provided options.
    /// </summary>
    /// <param name="options">The custom options to configure the OTP generator.</param>
    /// <returns>A securely generated random OTP string matching the specification.</returns>
    public static string Generate(OtpOptions options) => CurrentGenerator.Generate(options);

    /// <summary>
    /// Shorthand method to generate a standard, cryptographically secure numeric OTP of the specified length.
    /// </summary>
    /// <param name="length">The length of the generated code (must be between 4 and 32). Defaults to 6.</param>
    /// <returns>A string containing only numeric digits.</returns>
    public static string GenerateNumeric(int length = 6) => CurrentGenerator.GenerateNumeric(length);

    private static IOtpGenerator CurrentGenerator => ScopedGenerator.Value ?? DefaultGenerator;

    private sealed class GeneratorOverrideScope(IOtpGenerator? previous) : IDisposable
    {
        private IOtpGenerator? _previous = previous;
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            ScopedGenerator.Value = _previous;
            _previous = null;
            _disposed = true;
        }
    }
}
