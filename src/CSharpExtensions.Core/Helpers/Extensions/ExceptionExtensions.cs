namespace CSharpExtensions.Core.Helpers.Extensions;

/// <summary>
/// Bounded exception diagnostics for explicit local troubleshooting.
/// Returned messages and stack lines may contain sensitive data and are not safe for public logs or responses.
/// </summary>
public static class ExceptionExtensions
{
    /// <summary>
    /// Extracts all messages from the exception and its inner exceptions.
    /// </summary>
    public static List<string> GetMessages(this Exception exception)
        => GetMessages(exception, maxDepth: 16, maxMessages: 16);

    /// <summary>
    /// Extracts a bounded set of unique messages from the exception chain.
    /// </summary>
    public static List<string> GetMessages(this Exception exception, int maxDepth = 16, int maxMessages = 16)
    {
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDepth, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxMessages, 1);

        var messages = new List<string>();
        var uniqueMessages = new HashSet<string>(StringComparer.Ordinal);
        var current = exception;
        var depth = 0;

        while (current != null && depth++ < maxDepth && messages.Count < maxMessages)
        {
            if (!string.IsNullOrWhiteSpace(current.Message) && uniqueMessages.Add(current.Message))
            {
                messages.Add(current.Message);
            }
            current = current.InnerException;
        }

        return messages;
    }

    /// <summary>
    /// Gets application-level stack trace lines without allocating intermediate arrays.
    /// </summary>
    public static List<string> GetCleanStackTrace(this Exception exception)
        => GetCleanStackTrace(exception, maxDepth: 16, maxLines: 64);

    /// <summary>
    /// Gets a bounded set of unique application-level stack trace lines.
    /// </summary>
    public static List<string> GetCleanStackTrace(this Exception exception, int maxDepth = 16, int maxLines = 64)
    {
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDepth, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxLines, 1);

        var results = new List<string>();
        var uniqueLines = new HashSet<string>(StringComparer.Ordinal);
        var current = exception;
        var depth = 0;

        while (current != null && depth++ < maxDepth && results.Count < maxLines)
        {
            var stackTrace = current.StackTrace;
            if (string.IsNullOrWhiteSpace(stackTrace))
            {
                current = current.InnerException;
                continue;
            }

            foreach (var line in stackTrace.AsSpan().EnumerateLines())
            {
                if (line.Contains(":line ".AsSpan(), StringComparison.Ordinal))
                {
                    var cleaned = CleanLine(line);
                    if (uniqueLines.Add(cleaned))
                    {
                        results.Add(cleaned);
                        if (results.Count == maxLines)
                        {
                            break;
                        }
                    }
                }
            }
            current = current.InnerException;
        }

        return results;
    }

    private static string CleanLine(ReadOnlySpan<char> line)
    {
        var trimmed = line.Trim();
        var index = trimmed.IndexOf(") in ".AsSpan(), StringComparison.Ordinal);
        return index != -1 ? trimmed[(index + 5)..].ToString() : trimmed.ToString();
    }
}
