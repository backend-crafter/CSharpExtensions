using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace CSharpExtensions.Core.Railway;

/// <summary>
/// Provides global diagnostic and logging configuration for Railway Oriented Programming.
/// </summary>
public static class RailwayDiagnostics
{
    private static ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;
    private static ILogger _logger = NullLogger.Instance;

    /// <summary>
    /// Gets the configured logger factory.
    /// </summary>
    public static ILoggerFactory LoggerFactory => Volatile.Read(ref _loggerFactory);

    /// <summary>
    /// Gets the global logger instance for Railway operations.
    /// </summary>
    public static ILogger Logger => Volatile.Read(ref _logger);

    /// <summary>
    /// Configures the global logger factory and initializes the Railway logger.
    /// </summary>
    /// <param name="loggerFactory">The logger factory to configure.</param>
    public static void Configure(ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);
        Volatile.Write(ref _loggerFactory, loggerFactory);
        Volatile.Write(ref _logger, loggerFactory.CreateLogger("CSharpExtensions.Railway"));
    }
}
