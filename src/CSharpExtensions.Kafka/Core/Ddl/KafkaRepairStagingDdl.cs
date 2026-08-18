namespace CSharpExtensions.Kafka.Core.Ddl;

/// <summary>
/// Embedded DDL script generator for the kafka_repair_staging table.
/// </summary>
internal static class KafkaRepairStagingDdl
{
    /// <summary>
    /// Creates the DDL script for the kafka_repair_staging table.
    /// </summary>
    /// <param name="schema">The target database schema.</param>
    /// <returns>The DDL SQL string.</returns>
    public static string CreateTable(string schema)
    {
        var validatedSchema = SqlIdentifierValidator.ValidateIdentifier(schema, nameof(schema));
        return $@"
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'kafka_repair_staging' AND schema_id = SCHEMA_ID('{validatedSchema}'))
        BEGIN
            CREATE TABLE [{validatedSchema}].kafka_repair_staging
            (
                staging_id           BIGINT IDENTITY(1, 1) NOT NULL,
                partition_id         INT                   NOT NULL,
                message_offset       BIGINT                NOT NULL,
                message_key          VARCHAR(250)          NULL,
                raw_payload          NVARCHAR(MAX)         NOT NULL, 
                event_schema_version VARCHAR(100)          NULL,
                corrected_payload    NVARCHAR(MAX)         NULL,
                processing_status    VARCHAR(20)           NOT NULL DEFAULT 'Pending', 
                validation_error     NVARCHAR(MAX)         NULL,
                is_republished       BIT                   NOT NULL DEFAULT 0, 
                created_at           DATETIME2             NOT NULL DEFAULT (GETUTCDATE()),
                updated_at           DATETIME2             NOT NULL DEFAULT (GETUTCDATE()),

                CONSTRAINT PK_kafka_repair_staging PRIMARY KEY CLUSTERED (staging_id),
                CONSTRAINT UQ_kafka_repair_staging_partition_offset UNIQUE (partition_id, message_offset)
            );

            CREATE NONCLUSTERED INDEX IX_kafka_repair_staging_status_republished
                ON [{validatedSchema}].kafka_repair_staging (processing_status, is_republished)
                INCLUDE (partition_id, message_offset, message_key);
        END;";
    }
}
