using CSharpExtensions.Foundation.Railway;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace CSharpExtensions.Kafka.Core;

using RailwayError = Error;

/// <summary>
/// Redis-backed implementation of the distributed duplicate detector.
/// Uses <see cref="IRedisConnectionResolver"/> for multi-instance Redis support.
/// </summary>
public sealed class RedisDistributedDuplicateDetector : IDistributedDuplicateDetector, IDistributedDuplicateClaimRenewer
{
    internal const string InFlightErrorType = "Kafka.IdempotencyInFlight";
    internal const string LegacyOwnerToken = "legacy";
    private const int MaxMessageIdLength = 256;
    private const int MaxConsumerGroupLength = 255;
    private const int MaxOwnerTokenLength = 256;
    private const int MaxRetentionSeconds = 365 * 24 * 60 * 60;

    private readonly IRedisConnectionResolver _connectionResolver;
    private readonly KafkaOptions _options;
    private readonly ILogger<RedisDistributedDuplicateDetector>? _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisDistributedDuplicateDetector"/> class.
    /// </summary>
    /// <param name="connectionResolver">The Redis connection resolver.</param>
    /// <param name="options">The Kafka options containing idempotency settings.</param>
    /// <param name="logger">Optional logger instance.</param>
    public RedisDistributedDuplicateDetector(
        IRedisConnectionResolver connectionResolver,
        IOptions<KafkaOptions> options,
        ILogger<RedisDistributedDuplicateDetector>? logger = null)
    {
        _connectionResolver = connectionResolver ?? throw new ArgumentNullException(nameof(connectionResolver));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<Result<bool>> TryClaimUniqueAsync(
        string messageId,
        string consumerGroup,
        int retentionSeconds,
        CancellationToken cancellationToken = default,
        string? ownerToken = null)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!AreValidIdentifiers(messageId, consumerGroup)
            || !IsValidOwnerToken(ownerToken)
            || !IsValidRetention(retentionSeconds))
        {
            return Result.Failure<bool>(
                new RailwayError("Kafka idempotency claim parameters are invalid.")
                    .AsBadRequest("Kafka.IdempotencyInvalidInput", "Idempotency claim parameters are invalid."));
        }

        var connectionAlias = _options.Idempotency.RedisConnectionAlias;
        if (!_connectionResolver.IsRegistered(connectionAlias))
        {
            _logger?.LogError("Redis idempotency connection is unavailable; message processing is denied.");
            return Result.Failure<bool>(
                new RailwayError("Redis idempotency connection is unavailable.")
                    .AsInternalServer("Kafka.IdempotencyUnavailable", "Idempotency storage is unavailable."));
        }

        try
        {
            var connection = _connectionResolver.Resolve(connectionAlias);
            var database = connection.GetDatabase();
            var redisKey = $"idempotency:{consumerGroup}:{messageId}";

            var leaseSeconds = GetLeaseSeconds();
            const string claimLua = """
                local v = redis.call('GET', KEYS[1])
                if not v then
                    redis.call('SET', KEYS[1], 'Processing:' .. ARGV[2], 'EX', tonumber(ARGV[1]))
                    return 1
                end
                if v == 'Completed' then
                    return 0
                end
                local prefix = 'Processing:'
                if string.sub(v, 1, string.len(prefix)) == prefix then
                    local owner = string.sub(v, string.len(prefix) + 1)
                    if owner == ARGV[2] then
                        redis.call('SET', KEYS[1], v, 'EX', tonumber(ARGV[1]))
                        return 1
                    end
                    return 2
                end
                return 3
                """;

            var effectiveOwner = ownerToken ?? LegacyOwnerToken;
            var result = (int)(await database.ScriptEvaluateAsync(
                claimLua,
                keys: [new RedisKey(redisKey)],
                values: [new RedisValue(leaseSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture)), new RedisValue(effectiveOwner)]));

            return result switch
            {
                1 => Result.Success(true),
                0 => Result.Success(false),
                2 => Result.Failure<bool>(
                    new RailwayError("Message processing is already claimed by another consumer.")
                        .AsInternalServer(InFlightErrorType, "Message processing is already in progress.")),
                _ => Result.Failure<bool>(
                    new RailwayError("Redis idempotency state is invalid.")
                        .AsInternalServer("Kafka.IdempotencyInvalidState", "Idempotency state is invalid."))
            };
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger?.LogError(
                "Redis idempotency claim failed; message processing is denied. ErrorType: {ErrorType}.",
                exception.GetType().Name);
            return Result.Failure<bool>(
                new RailwayError("Redis idempotency claim failed.")
                    .AsInternalServer("Kafka.IdempotencyUnavailable", "Idempotency storage is unavailable."));
        }
    }

    /// <inheritdoc />
    public async Task<Result> CompleteClaimAsync(
        string messageId,
        string consumerGroup,
        CancellationToken cancellationToken = default,
        int retentionSeconds = 604800,
        string? ownerToken = null)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!AreValidIdentifiers(messageId, consumerGroup)
            || !IsValidOwnerToken(ownerToken)
            || !IsValidRetention(retentionSeconds))
        {
            return Result.Failure("Kafka idempotency completion parameters are invalid.");
        }

        var connectionAlias = _options.Idempotency.RedisConnectionAlias;
        if (!_connectionResolver.IsRegistered(connectionAlias))
        {
            return Result.Failure("Idempotency storage is unavailable.");
        }

        try
        {
            var connection = _connectionResolver.Resolve(connectionAlias);
            var database = connection.GetDatabase();
            var redisKey = $"idempotency:{consumerGroup}:{messageId}";

            // Transition from "Processing:ownerToken" to "Completed" with full retentionSeconds
            const string completeLua = """
                local v = redis.call('GET', KEYS[1])
                if not v then return 0 end
                if v == 'Processing:' .. ARGV[2] then
                    redis.call('SET', KEYS[1], 'Completed', 'EX', tonumber(ARGV[1]))
                    return 1
                end
                return 0
                """;

            var effectiveOwner = ownerToken ?? LegacyOwnerToken;
            var result = (int)(await database.ScriptEvaluateAsync(
                completeLua,
                keys: [new RedisKey(redisKey)],
                values: [new RedisValue(retentionSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture)), new RedisValue(effectiveOwner)]));

            return result == 1 
                ? Result.Success() 
                : Result.Failure("Failed to complete claim. Lease lost or owned by another instance.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger?.LogError(
                "Redis idempotency completion failed. ErrorType: {ErrorType}.",
                exception.GetType().Name);
            return Result.Failure("Idempotency claim completion failed.");
        }
    }

    /// <inheritdoc />
    public async Task<Result> RenewClaimAsync(
        string messageId,
        string consumerGroup,
        string ownerToken,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!AreValidIdentifiers(messageId, consumerGroup) || !IsValidOwnerToken(ownerToken))
        {
            return Result.Failure("Kafka idempotency renewal parameters are invalid.");
        }

        var connectionAlias = _options.Idempotency.RedisConnectionAlias;
        if (!_connectionResolver.IsRegistered(connectionAlias))
        {
            return Result.Failure("Idempotency storage is unavailable.");
        }

        try
        {
            var database = _connectionResolver.Resolve(connectionAlias).GetDatabase();
            var redisKey = $"idempotency:{consumerGroup}:{messageId}";
            var leaseSeconds = GetLeaseSeconds();
            const string renewLua = """
                local v = redis.call('GET', KEYS[1])
                if v == 'Processing:' .. ARGV[1] then
                    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
                    return 1
                end
                return 0
                """;

            var result = (int)(await database.ScriptEvaluateAsync(
                renewLua,
                keys: [new RedisKey(redisKey)],
                values: [new RedisValue(ownerToken), new RedisValue(leaseSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture))]));

            return result == 1
                ? Result.Success()
                : Result.Failure("Failed to renew claim. Lease lost or owned by another instance.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger?.LogError(
                "Redis idempotency lease renewal failed; processing cannot continue safely. ErrorType: {ErrorType}.",
                exception.GetType().Name);
            return Result.Failure("Idempotency lease renewal failed.");
        }
    }

    internal int GetLeaseSeconds()
    {
        var maxPollSeconds = Math.Max(1, (_options.Consumer.MaxPollIntervalMs + 999) / 1000);
        return Math.Clamp(maxPollSeconds + 30, 60, 3600);
    }

    /// <inheritdoc />
    public async Task<Result> ReleaseClaimAsync(
        string messageId,
        string consumerGroup,
        CancellationToken cancellationToken = default,
        string? ownerToken = null)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!AreValidIdentifiers(messageId, consumerGroup) || !IsValidOwnerToken(ownerToken))
        {
            return Result.Failure("Kafka idempotency release parameters are invalid.");
        }

        var connectionAlias = _options.Idempotency.RedisConnectionAlias;
        if (!_connectionResolver.IsRegistered(connectionAlias))
        {
            return Result.Failure("Idempotency storage is unavailable.");
        }

        try
        {
            var connection = _connectionResolver.Resolve(connectionAlias);
            var database = connection.GetDatabase();
            var redisKey = $"idempotency:{consumerGroup}:{messageId}";

            // Delete key if it is still owned by ownerToken in Processing state
            const string releaseLua = """
                local v = redis.call('GET', KEYS[1])
                if v == 'Processing:' .. ARGV[1] then
                    redis.call('DEL', KEYS[1])
                    return 1
                end
                return 0
                """;

            var effectiveOwner = ownerToken ?? LegacyOwnerToken;
            var result = (int)(await database.ScriptEvaluateAsync(
                releaseLua,
                keys: [new RedisKey(redisKey)],
                values: [new RedisValue(effectiveOwner)]));

            return result == 1
                ? Result.Success()
                : Result.Failure("Failed to release claim. Lease lost or owned by another instance.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger?.LogError(
                "Redis idempotency release failed. ErrorType: {ErrorType}.",
                exception.GetType().Name);
            return Result.Failure("Idempotency claim release failed.");
        }
    }

    private bool IsValidRetention(int retentionSeconds)
    {
        return retentionSeconds is > 0 and <= MaxRetentionSeconds;
    }

    private static bool AreValidIdentifiers(string messageId, string consumerGroup)
    {
        return IsSafeKeySegment(messageId, MaxMessageIdLength)
            && IsSafeKeySegment(consumerGroup, MaxConsumerGroupLength);
    }

    private static bool IsSafeKeySegment(string value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > maxLength)
        {
            return false;
        }

        foreach (var character in value)
        {
            if (character == ':' || char.IsControl(character))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsValidOwnerToken(string? ownerToken)
    {
        if (ownerToken is null)
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(ownerToken) || ownerToken.Length > MaxOwnerTokenLength)
        {
            return false;
        }

        foreach (var character in ownerToken)
        {
            if (char.IsControl(character))
            {
                return false;
            }
        }

        return true;
    }
}

