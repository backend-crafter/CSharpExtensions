using Microsoft.Data.SqlClient;

namespace CSharpExtensions.Foundation.Railway.Extensions;

/// <summary>
/// Provides extension methods to map SQL Server database exceptions to Railway Error instances.
/// </summary>
public static class DatabaseErrorExtensions
{
    /// <summary>
    /// Maps a mapping exception to a Database.MappingError Railway Error.
    /// </summary>
    public static Error CommandMappingError(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return new Error("Database command result could not be mapped.")
            .AsInternalServer("Database.MappingError", "Database result mapping failed.")
            .CausedBy(exception);
    }

    /// <summary>
    /// Maps a database exception to an appropriate Railway Error.
    /// </summary>
    public static Error DatabaseError(Exception exception, string? procedureName = null)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return exception switch
        {
            SqlException sqlException => DatabaseError(sqlException, procedureName),
            InvalidCastException invalidCastException => CommandMappingError(invalidCastException),
            _ => new Error("Database operation failed.")
                .AsInternalServer("Database.OperationError", "Database operation failed.")
                .CausedBy(exception)
        };
    }

    /// <summary>
    /// Maps a SqlException to a Database.SqlError Railway Error with metadata.
    /// </summary>
    public static Error DatabaseError(SqlException exception, string? procedureName = null)
    {
        ArgumentNullException.ThrowIfNull(exception);

        var error = new Error("Database operation failed.")
            .AsInternalServer("Database.SqlError", "Database operation failed.")
            .WithMetadata("Number", exception.Number)
            .WithMetadata("Class", (int)exception.Class);

        if (exception.SqlState != null)
        {
            error.WithMetadata("SqlState", exception.SqlState);
        }

        return error.CausedBy(exception);
    }
}
