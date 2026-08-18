namespace CSharpExtensions.Kafka.Core.Ddl;

/// <summary>
/// Embedded DDL scripts for the staged_resolve_jobs table.
/// </summary>
internal static class ResolveJobsDdl
{
    /// <summary>
    /// Creates the staged_resolve_jobs table if it does not exist.
    /// </summary>
    /// <param name="schema">The database schema name.</param>
    /// <returns>The DDL script to create the table and filtered index.</returns>
    public static string CreateTable(string schema)
    {
        var validatedSchema = SqlIdentifierValidator.ValidateIdentifier(schema, nameof(schema));
        return $@"
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'staged_resolve_jobs' AND schema_id = SCHEMA_ID('{validatedSchema}'))
        BEGIN
            CREATE TABLE [{validatedSchema}].staged_resolve_jobs
            (
                job_id              BIGINT IDENTITY(1,1)  NOT NULL,
                job_type            NVARCHAR(256)         NOT NULL,
                payload_json        NVARCHAR(MAX)         NOT NULL,
                status              NVARCHAR(32)          NOT NULL DEFAULT ('Pending'),
                attempt_count       INT                   NOT NULL DEFAULT (0),
                max_attempts        INT                   NOT NULL DEFAULT (5),
                next_attempt_at     DATETIME2             NOT NULL DEFAULT (GETUTCDATE()),
                locked_by           NVARCHAR(256)         NULL,
                error_message       NVARCHAR(MAX)         NULL,
                created_at          DATETIME2             NOT NULL DEFAULT (GETUTCDATE()),
                updated_at          DATETIME2             NOT NULL DEFAULT (GETUTCDATE()),

                CONSTRAINT PK_staged_resolve_jobs PRIMARY KEY CLUSTERED (job_id)
            );

            CREATE NONCLUSTERED INDEX IX_staged_resolve_jobs_polling
                ON [{validatedSchema}].staged_resolve_jobs (status, next_attempt_at)
                INCLUDE (job_type, attempt_count, max_attempts)
                WHERE status IN ('Pending', 'Retry');
        END;";
    }
}
