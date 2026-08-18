namespace CSharpExtensions.Foundation.Helpers.Options;

/// <summary>
/// Universal configuration map for named databases bound from the "Databases" section.
/// </summary>
public class DatabasesOptions : Dictionary<string, DatabaseOptions>
{
    public const string SectionName = "Databases";

    public DatabasesOptions() : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    public DatabasesOptions(IDictionary<string, DatabaseOptions> dictionary)
        : base(dictionary, StringComparer.OrdinalIgnoreCase)
    {
    }
}
