namespace CSharpExtensions.Core.Helpers.Options;

public class DatabaseOptions
{
    public string ConnectionString { get; set; } = string.Empty;
    
    public Dictionary<int, string> Shards { get; set; } = new();

    public ShardingOptions? Sharding { get; set; }
}
