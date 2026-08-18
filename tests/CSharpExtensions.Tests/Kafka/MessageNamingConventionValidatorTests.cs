using System.Text.Json.Serialization;
using CSharpExtensions.Kafka.Validation;
using Xunit;

namespace CSharpExtensions.Tests.Kafka;

public class MessageNamingConventionValidatorTests
{
    private class ValidV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }
        public string UserId { get; set; } = string.Empty;
        public string EmployeeId { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }

    private class InvalidPlayerIdV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }
        public string PlayerId { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }

    private class InvalidMemberIdV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }
        public string MemberId { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }

    private class InvalidPlayerNameV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }
        public string PlayerName { get; set; } = string.Empty;
    }

    private class InvalidUserPlayerV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }
        public string UserPlayer { get; set; } = string.Empty;
    }

    private class MissingPartnerIdV1Message
    {
        public int Version => 1;
        public string UserId { get; set; } = string.Empty;
    }

    private class InvalidSnakeCaseV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }
        public string account_number { get; set; } = string.Empty;
    }

    private class InvalidJsonPropertyNameV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }
        [JsonPropertyName("account-number")]
        public string AccountNumber { get; set; } = string.Empty;
    }

    private class InvalidClientIdV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }
        public string ClientId { get; set; } = string.Empty;
    }

    private class InvalidWebUserIdV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }
        public string WebUserId { get; set; } = string.Empty;
    }

    private class InvalidIsWebUserV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }
        public bool IsWebUser { get; set; }
    }

    private class ValidUserSpecialFieldsV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }
        public string[] Users { get; set; } = Array.Empty<string>();
        public string UserAgent { get; set; } = string.Empty;
        public string AccountId { get; set; } = string.Empty;
    }

    private class MissingVersionV1Message
    {
        public int PartnerId { get; set; }
    }

    private class MismatchedVersionV1Message
    {
        public int Version => 2; // Should be 1 based on V1 in class name
        public int PartnerId { get; set; }
    }

    private class ObsoleteSchemaVersionV1Message
    {
        public int Version => 1;
        public string SchemaVersion { get; set; } = "1.0";
        public int PartnerId { get; set; }
    }

    private class MissingVersionSuffixMessage
    {
        public int Version => 1;
        public int PartnerId { get; set; }
    }

    private class ThrowingConstructorV1Message
    {
        public int Version => 1;
        public int PartnerId { get; set; }

        public ThrowingConstructorV1Message()
        {
            throw new InvalidOperationException("The message constructor must not run during validation.");
        }
    }

    [Fact]
    public void Validate_WithValidMessage_ShouldNotThrow()
    {
        // Act & Assert
        MessageNamingConventionValidator.Validate<ValidV1Message>();
    }

    [Fact]
    public void Validate_DoesNotInvokeMessageConstructor()
    {
        MessageNamingConventionValidator.Validate<ThrowingConstructorV1Message>();
    }

    [Fact]
    public void Validate_WithInvalidPlayerId_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<InvalidPlayerIdV1Message>());

        Assert.Contains("Property 'PlayerId' on message type 'InvalidPlayerIdV1Message' uses a prohibited naming convention", exception.Message);
        Assert.Contains("Use 'UserId' (Client/User context) or 'EmployeeId' (Employee/Staff context)", exception.Message);
    }

    [Fact]
    public void Validate_WithInvalidMemberId_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<InvalidMemberIdV1Message>());

        Assert.Contains("Property 'MemberId' on message type 'InvalidMemberIdV1Message' uses a prohibited naming convention", exception.Message);
    }

    [Fact]
    public void Validate_WithInvalidPlayerName_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<InvalidPlayerNameV1Message>());

        Assert.Contains("Property 'PlayerName' on message type 'InvalidPlayerNameV1Message' uses a prohibited naming convention", exception.Message);
    }

    [Fact]
    public void Validate_WithInvalidUserPlayer_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<InvalidUserPlayerV1Message>());

        Assert.Contains("Property 'UserPlayer' on message type 'InvalidUserPlayerV1Message' uses a prohibited naming convention", exception.Message);
    }

    [Fact]
    public void Validate_WithMissingPartnerId_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<MissingPartnerIdV1Message>());

        Assert.Contains("must contain a 'PartnerId' property", exception.Message);
    }

    [Fact]
    public void Validate_WithSnakeCaseProperty_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<InvalidSnakeCaseV1Message>());

        Assert.Contains("violates the camelCase convention", exception.Message);
    }

    [Fact]
    public void Validate_WithKebabCaseJsonPropertyName_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<InvalidJsonPropertyNameV1Message>());

        Assert.Contains("violates the camelCase convention", exception.Message);
    }

    [Fact]
    public void Validate_WithClientId_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<InvalidClientIdV1Message>());

        Assert.Contains("uses a prohibited naming convention", exception.Message);
    }

    [Fact]
    public void Validate_WithWebUserId_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<InvalidWebUserIdV1Message>());

        Assert.Contains("uses a prohibited naming convention", exception.Message);
    }

    [Fact]
    public void Validate_WithIsWebUser_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<InvalidIsWebUserV1Message>());

        Assert.Contains("uses a prohibited naming convention", exception.Message);
    }

    [Fact]
    public void Validate_WithValidSpecialFields_ShouldNotThrow()
    {
        // Act & Assert
        MessageNamingConventionValidator.Validate<ValidUserSpecialFieldsV1Message>();
    }

    [Fact]
    public void Validate_WithMissingVersionProperty_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<MissingVersionV1Message>());

        Assert.Contains("must contain a read-only integer 'Version' property", exception.Message);
    }

    [Fact]
    public void Validate_WithMismatchedVersionProperty_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<MismatchedVersionV1Message>());

        Assert.Contains("version mismatch. The class name implies version 1, but the 'Version' property returns 2", exception.Message);
    }

    [Fact]
    public void Validate_WithObsoleteSchemaVersionProperty_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<ObsoleteSchemaVersionV1Message>());

        Assert.Contains("contains obsolete 'SchemaVersion' property", exception.Message);
    }

    [Fact]
    public void Validate_WithMissingVersionSuffix_ShouldThrowInvalidOperationException()
    {
        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<MissingVersionSuffixMessage>());

        Assert.Contains("must end with 'V[Version]Message'", exception.Message);
    }

    [Fact]
    public void Validate_WithNullType_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            MessageNamingConventionValidator.Validate(null!));
    }

    [Fact]
    public void Validate_WithValidPolicyV5Contract_DoesNotThrowOrLogErrors()
    {
        // Act & Assert (Should not throw and should pass standard validation)
        MessageNamingConventionValidator.Validate<ValidV5UserEventV1>();
    }

    [Fact]
    public void Validate_WithInvalidPolicyV5Contract_FailsStartupValidation()
    {
        Assert.Throws<InvalidOperationException>(() =>
            MessageNamingConventionValidator.Validate<InvalidV5UserEventV1>());
    }
}

public interface IUserEvent {}

public class ValidV5UserEventV1 : IUserEvent
{
    public const string MessageType = "Events";
    public const string Domain = "Orders";
    public const string Aggregate = "OrderHistory";
    public const string Action = "Changed";
    public int Version => 1;

    public Guid MessageId { get; set; }
    public DateTime OccurredAtUtc { get; set; }
    public int TenantId { get; set; }
    public int PartnerId { get; set; }
    public Guid UserId { get; set; }
}

public class InvalidV5UserEventV1 : IUserEvent
{
    // Missing metadata constants, and missing required properties
    public int PartnerId { get; set; }
}