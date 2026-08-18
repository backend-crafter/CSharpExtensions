using CSharpExtensions.Core.Railway;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

/// <summary>
/// Redis-backed implementation of <see cref="IMessageAssembler"/> using Hash structures and Lua scripting
/// for atomic segment storage and assembly verification.
/// </summary>
internal sealed class RedisMessageAssembler : IMessageAssembler
{
    private readonly IRedisConnectionResolver _redisConnectionResolver;
    private readonly KafkaOptions _options;
    private readonly ILogger<RedisMessageAssembler> _logger;

    /// <summary>
    /// Lua script that atomically:
    /// 1. HSET the segment payload
    /// 2. Sets TTL on the hash key
    /// 3. Checks HLEN against expected total
    /// 4. If complete: returns all hash fields/values and deletes the key
    /// 5. If partial: returns nil
    /// </summary>
    private const string AssemblyLuaScript = @"
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
        local count = redis.call('HLEN', KEYS[1])
        if count == tonumber(ARGV[4]) then
            local all = redis.call('HGETALL', KEYS[1])
            redis.call('DEL', KEYS[1])
            return all
        end
        return nil";

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisMessageAssembler"/> class.
    /// </summary>
    /// <param name="redisConnectionResolver">The Redis connection resolver for multi-instance support.</param>
    /// <param name="options">The Kafka options containing assembly configuration.</param>
    /// <param name="logger">The logger instance.</param>
    public RedisMessageAssembler(
        IRedisConnectionResolver redisConnectionResolver,
        IOptions<KafkaOptions> options,
        ILogger<RedisMessageAssembler> logger)
    {
        _redisConnectionResolver = redisConnectionResolver ?? throw new ArgumentNullException(nameof(redisConnectionResolver));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<Result<string?>> TryAssembleAsync(
        string segmentPayload,
        string assemblyKey,
        int segmentIndex,
        int totalSegments,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(segmentPayload))
            return Result.Failure<string?>("Segment payload cannot be null or empty.");

        if (string.IsNullOrWhiteSpace(assemblyKey))
            return Result.Failure<string?>("Assembly key cannot be null or empty.");

        if (segmentIndex < 0 || segmentIndex >= totalSegments)
            return Result.Failure<string?>($"Segment index {segmentIndex} is out of range [0, {totalSegments}).");

        if (totalSegments <= 0)
            return Result.Failure<string?>("Total segments must be greater than zero.");

        try
        {
            var assemblyOptions = _options.Assembly;
            var connectionAlias = assemblyOptions.RedisConnectionAlias;
            var multiplexer = _redisConnectionResolver.Resolve(connectionAlias);
            var database = multiplexer.GetDatabase();

            // Use Redis hash tag {assemblyKey} to ensure all segments for the same
            // aggregation key are co-located on the same shard in Redis Cluster mode
            var redisKey = $"kafka:assembly:{{{assemblyKey}}}";
            var staleThresholdSeconds = assemblyOptions.StaleThresholdSeconds;

            var result = await database.ScriptEvaluateAsync(
                AssemblyLuaScript,
                new RedisKey[] { redisKey },
                new RedisValue[]
                {
                    segmentIndex.ToString(),
                    segmentPayload,
                    staleThresholdSeconds.ToString(),
                    totalSegments.ToString()
                });

            // Lua returns nil when assembly is still partial
            if (result.IsNull)
            {
                _logger.LogDebug(
                    "Segment {SegmentIndex}/{TotalSegments} stored. Waiting for remaining segments.",
                    segmentIndex + 1,
                    totalSegments);

                return Result.Success<string?>(null);
            }

            // Lua returns HGETALL result as flat array: [field1, value1, field2, value2, ...]
            var flatArray = (RedisValue[])result!;
            var segments = new (int Index, string Payload)[flatArray.Length / 2];

            for (var i = 0; i < flatArray.Length; i += 2)
            {
                var index = int.Parse(flatArray[i]!);
                var payload = (string)flatArray[i + 1]!;
                segments[i / 2] = (index, payload);
            }

            // Sort by segment index and concatenate
            var assembledPayload = string.Concat(
                segments
                    .OrderBy(segment => segment.Index)
                    .Select(segment => segment.Payload));

            _logger.LogInformation(
                "Successfully assembled {TotalSegments} Kafka segments. Total payload length: {PayloadLength}.",
                totalSegments,
                assembledPayload.Length);

            return Result.Success<string?>(assembledPayload);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (RedisConnectionException exception)
        {
            _logger.LogError("Redis Kafka assembly failed. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure<string?>("Redis Kafka message assembly failed.");
        }
        catch (Exception exception)
        {
            _logger.LogError("Unexpected Kafka assembly failure. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure<string?>("Kafka message assembly failed.");
        }
    }
}
