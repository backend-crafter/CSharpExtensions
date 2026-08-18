using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace CSharpExtensions.Foundation.Helpers.Options;

public sealed class ShardingOptionsValidator : IValidateOptions<ShardingOptions>
{
    public ValidateOptionsResult Validate(string? name, ShardingOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = ValidateValues(options).ToArray();
        return failures.Length == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }

    internal static IEnumerable<string> ValidateValues(ShardingOptions options)
    {
        if (options.LogicalShardCount is <= 0 or > 65_536 ||
            (options.LogicalShardCount & (options.LogicalShardCount - 1)) != 0)
        {
            yield return "LogicalShardCount must be a power of two between 1 and 65536.";
        }

        if (options.HotReadModelRetentionDays is <= 0 or > 3_650)
        {
            yield return "HotReadModelRetentionDays must be between 1 and 3650.";
        }

        if (options.CleanupBatchSize is <= 0 or > 100_000)
        {
            yield return "CleanupBatchSize must be between 1 and 100000.";
        }

        if (options.CleanupDelayBetweenBatchesMs is < 0 or > 60_000)
        {
            yield return "CleanupDelayBetweenBatchesMs must be between 0 and 60000.";
        }
    }
}

public sealed class DatabasesOptionsValidator : IValidateOptions<DatabasesOptions>
{
    private readonly HashSet<string>? _configuredSections;

    /// <summary>
    /// Creates a validator that validates every known database entry.
    /// </summary>
    public DatabasesOptionsValidator()
    {
    }

    /// <summary>
    /// Creates a validator that validates only database entries present in configuration.
    /// </summary>
    public DatabasesOptionsValidator(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var databasesSection = configuration.GetSection(DatabasesOptions.SectionName);
        _configuredSections = databasesSection
            .GetChildren()
            .Where(static section => section.Exists())
            .Select(static section => section.Key)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    public ValidateOptionsResult Validate(string? name, DatabasesOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        if (_configuredSections is not null)
        {
            foreach (var sectionKey in _configuredSections)
            {
                options.TryGetValue(sectionKey, out var database);
                ValidateDatabase(sectionKey, database, failures);
            }
        }
        else
        {
            foreach (var (optionName, database) in options)
            {
                ValidateDatabase(optionName, database, failures);
            }
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }

    private static void ValidateDatabase(
        string optionName,
        DatabaseOptions? database,
        ICollection<string> failures)
    {
        if (database is null)
        {
            failures.Add($"{optionName} configuration is required.");
            return;
        }

        if (string.IsNullOrWhiteSpace(database.ConnectionString) && (database.Shards is null || database.Shards.Count == 0))
        {
            failures.Add($"{optionName} requires a connection string or at least one shard connection.");
        }

        foreach (var shard in database.Shards ?? [])
        {
            if (shard.Key < 0 || string.IsNullOrWhiteSpace(shard.Value))
            {
                failures.Add($"{optionName} contains an invalid shard identifier or connection string.");
                break;
            }
        }

        if (database.Sharding is not null)
        {
            foreach (var failure in ShardingOptionsValidator.ValidateValues(database.Sharding))
            {
                failures.Add($"{optionName}.{failure}");
            }
        }
    }
}
