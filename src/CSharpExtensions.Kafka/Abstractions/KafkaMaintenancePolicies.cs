namespace CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Deny-by-default authorization policies for Kafka maintenance endpoints.
/// </summary>
public static class KafkaMaintenancePolicies
{
    public const string Read = "CSharpExtensions.Kafka.Maintenance.Read";
    public const string Write = "CSharpExtensions.Kafka.Maintenance.Write";
    public const string PermissionClaim = "kafka.maintenance";
    public const string ReadPermission = "read";
    public const string WritePermission = "write";
}
