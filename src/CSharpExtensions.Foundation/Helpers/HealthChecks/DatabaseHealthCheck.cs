using CSharpExtensions.Foundation.Helpers.Options;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace CSharpExtensions.Foundation.Helpers.HealthChecks;

public abstract class DatabaseHealthCheck(DatabaseOptions databaseOptions) : IHealthCheck
{
    private readonly DatabaseOptions _databaseOptions = databaseOptions ?? throw new ArgumentNullException(nameof(databaseOptions));

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var connectionString = _databaseOptions.ConnectionString;
        try
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                return HealthCheckResult.Unhealthy("SQL Server connection string is missing.");
            }

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            command.CommandTimeout = 5;
            await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

            return HealthCheckResult.Healthy();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return HealthCheckResult.Unhealthy("SQL Server dependency check failed.");
        }
    }
}
