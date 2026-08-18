using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core.Ddl;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// SQL Server-backed implementation of <see cref="IMessageAssembler"/> using the pending_message_assemblies table.
/// Uses SERIALIZABLE isolation transactions for atomicity when checking and retrieving assembled segments.
/// </summary>
internal sealed class SqlServerMessageAssembler : IMessageAssembler
{
    private readonly string? _connectionString;
    private readonly KafkaOptions _options;
    private readonly ILogger<SqlServerMessageAssembler> _logger;
    private bool _tableProvisioned;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlServerMessageAssembler"/> class.
    /// </summary>
    /// <param name="configuration">The application configuration for resolving connection strings.</param>
    /// <param name="options">The Kafka options containing assembly configuration.</param>
    /// <param name="logger">The logger instance.</param>
    public SqlServerMessageAssembler(
        IConfiguration configuration,
        IOptions<KafkaOptions> options,
        ILogger<SqlServerMessageAssembler> logger)
    {
        if (configuration is null) throw new ArgumentNullException(nameof(configuration));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var connectionStringName = _options.Assembly.ConnectionStringName;
        _connectionString = configuration.GetConnectionString(connectionStringName) ?? configuration[connectionStringName];
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

        if (string.IsNullOrWhiteSpace(_connectionString))
            return Result.Failure<string?>("SQL Server connection string is not configured for message assembly.");

        try
        {
            await EnsureTableProvisionedAsync(cancellationToken);

            var schema = SqlIdentifierValidator.ValidateIdentifier(_options.Assembly.TableSchema, nameof(_options.Assembly.TableSchema));

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
            try
            {
                // Insert the segment (MERGE to handle retries/duplicates gracefully)
                var insertSql = $@"
                    MERGE [{schema}].pending_message_assemblies AS target
                    USING (VALUES (@AssemblyKey, @SegmentIndex, @TotalSegments, @SegmentPayload)) 
                        AS source (assembly_key, segment_index, total_segments, segment_payload)
                    ON target.assembly_key = source.assembly_key AND target.segment_index = source.segment_index
                    WHEN NOT MATCHED THEN
                        INSERT (assembly_key, segment_index, total_segments, segment_payload)
                        VALUES (source.assembly_key, source.segment_index, source.total_segments, source.segment_payload)
                    WHEN MATCHED THEN
                        UPDATE SET segment_payload = source.segment_payload;";

                await connection.ExecuteAsync(
                    new CommandDefinition(
                        insertSql,
                        new
                        {
                            AssemblyKey = assemblyKey,
                            SegmentIndex = segmentIndex,
                            TotalSegments = totalSegments,
                            SegmentPayload = segmentPayload
                        },
                        transaction: transaction,
                        cancellationToken: cancellationToken));

                // Check if all segments have been received
                var countSql = $@"
                    SELECT COUNT(*) 
                    FROM [{schema}].pending_message_assemblies 
                    WHERE assembly_key = @AssemblyKey;";

                var receivedCount = await connection.ExecuteScalarAsync<int>(
                    new CommandDefinition(
                        countSql,
                        new { AssemblyKey = assemblyKey },
                        transaction: transaction,
                        cancellationToken: cancellationToken));

                if (receivedCount < totalSegments)
                {
                    transaction.Commit();

                    _logger.LogDebug(
                        "Segment {SegmentIndex}/{TotalSegments} stored. Received {ReceivedCount} of {TotalSegments} segments.",
                        segmentIndex + 1,
                        totalSegments,
                        receivedCount,
                        totalSegments);

                    return Result.Success<string?>(null);
                }

                // All segments received - retrieve them in order
                var selectSql = $@"
                    SELECT segment_payload 
                    FROM [{schema}].pending_message_assemblies 
                    WHERE assembly_key = @AssemblyKey 
                    ORDER BY segment_index ASC;";

                var segments = await connection.QueryAsync<string>(
                    new CommandDefinition(
                        selectSql,
                        new { AssemblyKey = assemblyKey },
                        transaction: transaction,
                        cancellationToken: cancellationToken));

                var assembledPayload = string.Concat(segments);

                // Clean up all segments for this assembly
                var deleteSql = $@"
                    DELETE FROM [{schema}].pending_message_assemblies 
                    WHERE assembly_key = @AssemblyKey;";

                await connection.ExecuteAsync(
                    new CommandDefinition(
                        deleteSql,
                        new { AssemblyKey = assemblyKey },
                        transaction: transaction,
                        cancellationToken: cancellationToken));

                transaction.Commit();

                _logger.LogInformation(
                    "Successfully assembled {TotalSegments} segments. Total payload length: {PayloadLength}.",
                    totalSegments,
                    assembledPayload.Length);

                return Result.Success<string?>(assembledPayload);
            }
            catch
            {
                try { transaction.Rollback(); } catch { /* Suppress rollback errors */ }
                throw;
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError("SQL Server Kafka assembly failed. ErrorType: {ErrorType}.", exception.GetType().Name);
            return Result.Failure<string?>("SQL Server Kafka message assembly failed.");
        }
    }

    /// <summary>
    /// Ensures the pending_message_assemblies table exists in the target database.
    /// Only runs once per service lifetime.
    /// </summary>
    private async Task EnsureTableProvisionedAsync(CancellationToken cancellationToken)
    {
        if (_tableProvisioned || !_options.Assembly.AutoProvisionDdl)
            return;

        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            var schema = SqlIdentifierValidator.ValidateIdentifier(_options.Assembly.TableSchema, nameof(_options.Assembly.TableSchema));
            var ddl = MessageAssembliesDdl.CreateTable(schema);

            await connection.ExecuteAsync(
                new CommandDefinition(ddl, cancellationToken: cancellationToken));

            _logger.LogInformation(
                "Message assembly table [{Schema}].pending_message_assemblies is provisioned and ready.",
                schema);

            _tableProvisioned = true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                "Failed to auto-provision the Kafka assembly table. ErrorType: {ErrorType}.",
                exception.GetType().Name);
            throw new InvalidOperationException("Failed to provision the Kafka assembly table.");
        }
    }
}
