using System.Data;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using CSharpExtensions.Core.Helpers;
using CSharpExtensions.Core.Json;
using CSharpExtensions.Core.Railway;
using CSharpExtensions.Kafka.Abstractions;
using Dapper;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.Kafka.Core;

/// <summary>
/// Db-backed publisher saving messages inside the active database transaction.
/// </summary>
public sealed class KafkaOutboxPublisher : IOutboxPublisher
{
    private const int MaximumConfigurationKeyCharacters = 200;
    private const int MaximumDatabaseMessageKeyCharacters = 500;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private readonly string _tableSchema;
    private readonly int _maxAttempts;
    private readonly int _maxPayloadBytes;
    private readonly int _maxMessageKeyBytes;

    /// <summary>
    /// Initializes a publisher using the default <c>dbo</c> schema.
    /// </summary>
    public KafkaOutboxPublisher()
        : this(Options.Create(new KafkaOptions()))
    {
    }

    /// <summary>
    /// Initializes a publisher using the configured outbox schema.
    /// </summary>
    /// <param name="options">The validated Kafka options.</param>
    public KafkaOutboxPublisher(IOptions<KafkaOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _tableSchema = SqlIdentifierValidator.ValidateIdentifier(
            options.Value.Outbox.TableSchema,
            nameof(KafkaOutboxSettings.TableSchema));
        _maxAttempts = options.Value.Outbox.MaxAttempts;
        _maxPayloadBytes = options.Value.Producer.MaxPayloadBytes;
        _maxMessageKeyBytes = options.Value.Producer.MaxMessageKeyBytes;
    }

    /// <inheritdoc />
    public async Task<Result> EnqueueAsync<TMessage>(
        TMessage message,
        IDbTransaction dbTransaction,
        string? messageKey = null,
        CancellationToken cancellationToken = default) where TMessage : class
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }
        if (dbTransaction is null)
        {
            throw new ArgumentNullException(nameof(dbTransaction));
        }

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            var connection = dbTransaction.Connection;
            if (connection is null)
            {
                return Result.Failure("The database transaction connection is invalid or closed.");
            }

            var configurationKey = TopicAttributeResolver.Resolve<TMessage>();
            if (!TryNormalizeConfigurationKey(configurationKey, out configurationKey))
            {
                return Result.Failure("Kafka outbox configuration key is invalid.");
            }

            if (!IsMessageKeyWithinLimits(messageKey, _maxMessageKeyBytes))
            {
                return Result.Failure("Kafka outbox message key exceeds the permitted limit.");
            }

            var messageId = Guid.NewGuid().ToString();
            
            // Extract or initialize correlation identifier
            var correlationId = Activity.Current?.RootId ?? Guid.NewGuid().ToString();

            var payloadJson = JsonSerializer.Serialize(message, JsonOptions.KafkaCompatible);
            if (!IsPayloadWithinLimit(payloadJson, _maxPayloadBytes))
            {
                return Result.Failure("Kafka outbox payload exceeds the configured producer limit.");
            }

            var sql = BuildInsertSql(_tableSchema);

            var affectedRows = await connection.ExecuteAsync(
                new CommandDefinition(
                    sql,
                    new
                    {
                        MessageId = messageId,
                        CorrelationId = correlationId,
                        ConfigurationKey = configurationKey,
                        MessageKey = messageKey,
                        PayloadJson = payloadJson,
                        MaxAttempts = _maxAttempts
                    },
                    transaction: dbTransaction,
                    cancellationToken: cancellationToken));

            if (affectedRows > 0)
            {
                return Result.Success();
            }

            return Result.Failure("Failed to insert message into outbox table.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            return Result.Failure($"Failed to enqueue outbox message. ErrorType: {exception.GetType().Name}.");
        }
    }

    internal static bool IsMessageKeyWithinLimits(string? messageKey, int maximumBytes)
    {
        if (messageKey is null)
        {
            return true;
        }

        return maximumBytes > 0
            && messageKey.Length <= MaximumDatabaseMessageKeyCharacters
            && StrictUtf8.GetByteCount(messageKey) <= maximumBytes;
    }

    internal static bool TryNormalizeConfigurationKey(string? configurationKey, out string normalized)
    {
        return BoundedIdentifier.TryNormalize(
            configurationKey,
            out normalized,
            MaximumConfigurationKeyCharacters);
    }

    internal static bool IsPayloadWithinLimit(string payloadJson, int maximumBytes)
    {
        return maximumBytes > 0
            && !string.IsNullOrEmpty(payloadJson)
            && StrictUtf8.GetByteCount(payloadJson) <= maximumBytes;
    }

    internal static string BuildInsertSql(string tableSchema)
    {
        var schema = SqlIdentifierValidator.ValidateIdentifier(
            tableSchema,
            nameof(KafkaOutboxSettings.TableSchema));

        return $@"
                INSERT INTO [{schema}].kafka_outbox (
                    message_id,
                    correlation_id,
                    configuration_key,
                    message_key,
                    payload_json,
                    processing_status,
                    attempt_count,
                    max_attempts,
                    created_at,
                    updated_at
                ) VALUES (
                    @MessageId,
                    @CorrelationId,
                    @ConfigurationKey,
                    @MessageKey,
                    @PayloadJson,
                    'Pending',
                    0,
                    @MaxAttempts,
                    GETUTCDATE(),
                    GETUTCDATE()
                );";
    }
}
