namespace CSharpExtensions.Kafka.Core.Ddl;

/// <summary>
/// Embedded DDL scripts for the pending_message_assemblies table.
/// </summary>
internal static class MessageAssembliesDdl
{
    /// <summary>
    /// Creates the pending_message_assemblies table if it does not exist.
    /// </summary>
    /// <param name="schema">The database schema name.</param>
    /// <returns>The DDL script to create the table and indexes.</returns>
    public static string CreateTable(string schema)
    {
        var validatedSchema = SqlIdentifierValidator.ValidateIdentifier(schema, nameof(schema));
        return $@"
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'pending_message_assemblies' AND schema_id = SCHEMA_ID('{validatedSchema}'))
        BEGIN
            CREATE TABLE [{validatedSchema}].pending_message_assemblies
            (
                assembly_id         BIGINT IDENTITY(1,1) NOT NULL,
                assembly_key        NVARCHAR(256)        NOT NULL,
                segment_index       INT                  NOT NULL,
                total_segments      INT                  NOT NULL,
                segment_payload     NVARCHAR(MAX)        NOT NULL,
                created_at          DATETIME2            NOT NULL DEFAULT (GETUTCDATE()),

                CONSTRAINT PK_pending_message_assemblies PRIMARY KEY CLUSTERED (assembly_id),
                CONSTRAINT UQ_pending_message_assemblies_segment UNIQUE (assembly_key, segment_index)
            );

            CREATE NONCLUSTERED INDEX IX_pending_message_assemblies_key
                ON [{validatedSchema}].pending_message_assemblies (assembly_key)
                INCLUDE (segment_index, total_segments);
        END;";
    }
}
