using CSharpExtensions.Core.Railway;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Evolution;

namespace CSharpExtensions.Kafka.Core;

/// <summary>
/// Registry responsible for finding and executing upcaster chains to transform obsolete message formats.
/// </summary>
public sealed class MessageUpcastRegistry
{
    private readonly List<IMessageUpcaster> _upcasters;
    private readonly List<ISchemaDetector> _detectors;

    public MessageUpcastRegistry(IEnumerable<IMessageUpcaster> upcasters, IEnumerable<ISchemaDetector>? detectors = null)
    {
        _upcasters = upcasters?.ToList() ?? new List<IMessageUpcaster>();
        _detectors = detectors?.ToList() ?? new List<ISchemaDetector>();
    }

    /// <summary>
    /// Processes a historical payload through matching upcaster chains until the target schema is reached.
    /// </summary>
    /// <param name="rawPayloadJson">The obsolete JSON payload.</param>
    /// <param name="sourceSchemaKey">The schema key of the incoming message.</param>
    /// <param name="targetSchemaKey">The expected schema key of the consumer.</param>
    /// <returns>A Result wrapping the transformed JSON payload.</returns>
    public Result<string> UpcastMessage(string rawPayloadJson, string sourceSchemaKey, string targetSchemaKey)
    {
        if (rawPayloadJson is null) throw new ArgumentNullException(nameof(rawPayloadJson));
        if (sourceSchemaKey is null) throw new ArgumentNullException(nameof(sourceSchemaKey));
        if (targetSchemaKey is null) throw new ArgumentNullException(nameof(targetSchemaKey));
        if (sourceSchemaKey.Length > 256 || targetSchemaKey.Length > 256)
        {
            return Result.Failure<string>("Kafka schema key exceeds the permitted size.");
        }

        try
        {
            if (string.Equals(sourceSchemaKey, targetSchemaKey, StringComparison.Ordinal))
            {
                var detector = _detectors.FirstOrDefault(d => string.Equals(d.TargetSchemaKey, targetSchemaKey, StringComparison.Ordinal));
                if (detector != null && !detector.IsTargetSchema(rawPayloadJson))
                {
                    var detectedSource = detector.DetectSourceSchema(rawPayloadJson);
                    if (detectedSource != null && !string.Equals(detectedSource, targetSchemaKey, StringComparison.Ordinal))
                    {
                        sourceSchemaKey = detectedSource;
                    }
                    else
                    {
                        return Result.Success(rawPayloadJson);
                    }
                }
                else
                {
                    return Result.Success(rawPayloadJson);
                }
            }
        }
        catch (Exception)
        {
            return Result.Failure<string>("Kafka schema detection failed.");
        }

        var path = FindUpcastPath(sourceSchemaKey, targetSchemaKey);
        if (path is null || !path.Any())
        {
            return Result.Failure<string>(
                "No Kafka upcaster path resolved for the message schema.");
        }

        var currentPayload = rawPayloadJson;
        foreach (var upcaster in path)
        {
            try
            {
                currentPayload = upcaster.Upcast(currentPayload);
            }
            catch (Exception)
            {
                return Result.Failure<string>("Kafka schema transformation failed.");
            }
        }

        return Result.Success(currentPayload);
    }

    private List<IMessageUpcaster>? FindUpcastPath(string startSchemaKey, string targetSchemaKey)
    {
        var queue = new Queue<string>();
        queue.Enqueue(startSchemaKey);
        var visited = new HashSet<string>(StringComparer.Ordinal) { startSchemaKey };
        var predecessors = new Dictionary<string, (string Previous, IMessageUpcaster Upcaster)>(StringComparer.Ordinal);

        while (queue.Count > 0)
        {
            var currentKey = queue.Dequeue();

            if (string.Equals(currentKey, targetSchemaKey, StringComparison.Ordinal))
            {
                var path = new List<IMessageUpcaster>();
                var cursor = targetSchemaKey;
                while (!string.Equals(cursor, startSchemaKey, StringComparison.Ordinal))
                {
                    var predecessor = predecessors[cursor];
                    path.Add(predecessor.Upcaster);
                    cursor = predecessor.Previous;
                }
                path.Reverse();
                return path;
            }

            // Find all outgoing upcasters from the current version key
            foreach (var upcaster in _upcasters)
            {
                string nextKey;
                bool isMatch = false;

                if (string.Equals(upcaster.SourceSchemaKey, currentKey, StringComparison.Ordinal))
                {
                    nextKey = upcaster.TargetSchemaKey;
                    isMatch = true;
                }
                else if (MessageVersionResolver.HasVersionSuffix(currentKey))
                {
                    var versionIndex = currentKey.LastIndexOf(".v", StringComparison.Ordinal);
                    if (versionIndex > 0
                        && string.Equals(upcaster.SourceSchemaKey, currentKey[..versionIndex], StringComparison.Ordinal))
                    {
                        nextKey = upcaster.TargetSchemaKey + currentKey[versionIndex..];
                        isMatch = true;
                    }
                    else
                    {
                        nextKey = string.Empty;
                    }
                }
                else
                {
                    nextKey = string.Empty;
                }

                if (isMatch && !visited.Contains(nextKey))
                {
                    visited.Add(nextKey);
                    predecessors[nextKey] = (currentKey, upcaster);
                    queue.Enqueue(nextKey);
                }
            }
        }

        return null; // No path resolved
    }
}
