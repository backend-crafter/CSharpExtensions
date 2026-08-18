using CSharpExtensions.Foundation.Helpers.Constants;
using CSharpExtensions.Foundation.Security.Interfaces;

namespace CSharpExtensions.Tests.Kafka;

using System.Text;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

public sealed class KafkaTopicValidatorTests
{
    private const string Payload = "{\"data\":\"value\"}";
    private const string MessageId = "msg-123";
    private const string CorrelationId = "corr-456";

    private readonly SignatureService _signatureService;
    private readonly KafkaTopicValidator _validator;

    public KafkaTopicValidatorTests()
    {
        var encryptionService = new Mock<IEncryptionService>();
        encryptionService
            .Setup(service => service.Encrypt(It.IsAny<string>()))
            .Returns<string>(value => $"encrypted_{value}");
        encryptionService
            .Setup(service => service.Decrypt(It.IsAny<string>()))
            .Returns<string>(cipherText => cipherText.StartsWith("encrypted_", StringComparison.Ordinal)
                ? cipherText["encrypted_".Length..]
                : cipherText);

        _signatureService = new SignatureService(encryptionService.Object);
        _validator = new KafkaTopicValidator(
            Options.Create(new KafkaOptions()),
            _signatureService,
            Mock.Of<ILogger<KafkaTopicValidator>>());
    }

    [Fact]
    public void ValidateMessage_AuthenticationEnabledWithValidSignature_ReturnsNoErrors()
    {
        var signature = _signatureService.SignMessage(Payload, MessageId, CorrelationId);
        var message = CreateMessage(signature);

        var errors = _validator.ValidateMessage(message, authenticationEnabled: true);

        Assert.Empty(errors);
    }

    [Fact]
    public void ValidateMessage_AuthenticationEnabledWithInvalidSignature_ReturnsInvalidSignature()
    {
        var signature = _signatureService.SignMessage(Payload, MessageId, CorrelationId);
        var message = CreateMessage(signature, payload: "{\"data\":\"tampered\"}");

        var errors = _validator.ValidateMessage(message, authenticationEnabled: true);

        var error = Assert.Single(errors);
        Assert.Equal("InvalidSignature", error.ErrorCategory);
        Assert.Equal("Message signature verification failed.", error.ErrorMessage);
    }

    [Fact]
    public void ValidateMessage_AuthenticationEnabledWithoutSignature_ReturnsMissingHeader()
    {
        var message = CreateMessage(signature: null);

        var errors = _validator.ValidateMessage(message, authenticationEnabled: true);

        var error = Assert.Single(errors);
        Assert.Equal("MissingHeader", error.ErrorCategory);
        Assert.Contains(CustomRequestHeaders.MessageSignature, error.ErrorMessage, StringComparison.Ordinal);
    }

    private static ConsumeResult<string, string> CreateMessage(
        string? signature,
        string payload = Payload)
    {
        var headers = new Headers();
        AddHeader(headers, CustomRequestHeaders.MessageId, MessageId);
        AddHeader(headers, CustomRequestHeaders.CorrelationId, CorrelationId);
        AddHeader(headers, CustomRequestHeaders.EventSchemaVersion, "1");

        if (signature is not null)
        {
            AddHeader(headers, CustomRequestHeaders.MessageSignature, signature);
        }

        return new ConsumeResult<string, string>
        {
            Topic = "test.events.created.v1",
            Partition = new Partition(0),
            Offset = new Offset(42),
            Message = new Message<string, string>
            {
                Key = "key",
                Value = payload,
                Headers = headers
            }
        };
    }

    private static void AddHeader(Headers headers, string key, string value)
    {
        headers.Add(key, Encoding.UTF8.GetBytes(value));
    }
}
