using CSharpExtensions.Foundation.Security.Interfaces;
using CSharpExtensions.Kafka.Core;

namespace CSharpExtensions.Tests.Kafka;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

public sealed class SignatureServiceTests
{
    private readonly Mock<IEncryptionService> _mockEncryptionService;
    private readonly SignatureService _signatureService;

    public SignatureServiceTests()
    {
        _mockEncryptionService = new Mock<IEncryptionService>();
        
        // Mock simple encryption behavior
        _mockEncryptionService
            .Setup(service => service.Encrypt(It.IsAny<string>()))
            .Returns<string>(value => $"encrypted_{value}");
            
        _mockEncryptionService
            .Setup(service => service.Decrypt(It.IsAny<string>()))
            .Returns<string>(cipherText => cipherText.Replace("encrypted_", ""));

        _signatureService = new SignatureService(_mockEncryptionService.Object);
    }

    [Fact]
    public void SignMessage_WithValidInputs_ProducesEncryptedSignature()
    {
        // Arrange
        var payload = "{\"data\":\"value\"}";
        var messageId = "msg-123";
        var correlationId = "corr-456";

        // Act
        var signature = _signatureService.SignMessage(payload, messageId, correlationId);

        // Assert
        Assert.NotNull(signature);
        Assert.StartsWith("encrypted_", signature);
        _mockEncryptionService.Verify(service => service.Encrypt(It.IsAny<string>()), Times.Once);
    }

    [Fact]
    public void VerifySignature_WithValidSignature_ReturnsTrue()
    {
        // Arrange
        var payload = "{\"data\":\"value\"}";
        var messageId = "msg-123";
        var correlationId = "corr-456";
        var signature = _signatureService.SignMessage(payload, messageId, correlationId);

        // Act
        var isValid = _signatureService.VerifySignature(payload, messageId, correlationId, signature);

        // Assert
        Assert.True(isValid);
    }

    [Fact]
    public void VerifySignature_WithTamperedPayload_ReturnsFalse()
    {
        // Arrange
        var payload = "{\"data\":\"value\"}";
        var messageId = "msg-123";
        var correlationId = "corr-456";
        var signature = _signatureService.SignMessage(payload, messageId, correlationId);

        // Act
        var tamperedPayload = "{\"data\":\"tampered\"}";
        var isValid = _signatureService.VerifySignature(tamperedPayload, messageId, correlationId, signature);

        // Assert
        Assert.False(isValid);
    }

    [Fact]
    public void VerifySignature_WithInvalidSignatureString_ReturnsFalse()
    {
        // Arrange
        var payload = "{\"data\":\"value\"}";
        var messageId = "msg-123";
        var correlationId = "corr-456";

        // Act
        var isValid = _signatureService.VerifySignature(payload, messageId, correlationId, "invalid-sig");

        // Assert
        Assert.False(isValid);
    }

    [Fact]
    public void SignAndVerifyMessage_WithHmacV2_UsesVersionedSignature()
    {
        var key = new byte[32] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 };
        var keyProvider = new FixedKeyProvider("primary", key);
        var options = Options.Create(new KafkaOptions
        {
            Security = new KafkaSecuritySettings
            {
                SignatureWriteVersion = KafkaSignatureWriteVersion.HmacSha256V2,
                AllowLegacyV1Verification = true
            }
        });
        var service = new SignatureService(_mockEncryptionService.Object, options, keyProvider);

        var signature = service.SignMessage(
            "{\"data\":\"value\"}",
            "msg-123",
            "corr-456",
            "env.domain.entity.event.v1",
            "entity-1",
            "Event.v1",
            KafkaEnvelopeKinds.Inline);

        Assert.Equal("v2.primary.-QIbYDmqvKDpVdOVy345bwtW0LXJYlXftyN3taL20A0", signature);
        Assert.True(service.VerifySignature(
            "{\"data\":\"value\"}", "msg-123", "corr-456", "env.domain.entity.event.v1",
            "entity-1", "Event.v1", KafkaEnvelopeKinds.Inline, signature));
        Assert.False(service.VerifySignature(
            "{\"data\":\"value\"}", "msg-123", "corr-456", "env.domain.entity.event.v2",
            "entity-1", "Event.v1", KafkaEnvelopeKinds.Inline, signature));
        Assert.False(service.VerifySignature(
            "{\"data\":\"value\"}", "msg-123", "corr-456", "env.domain.entity.event.v1",
            "entity-2", "Event.v1", KafkaEnvelopeKinds.Inline, signature));
        Assert.False(service.VerifySignature(
            "{\"data\":\"value\"}", "msg-123", "corr-456", "env.domain.entity.event.v1",
            "entity-1", "Event.v2", KafkaEnvelopeKinds.Inline, signature));
        Assert.False(service.VerifySignature(
            "{\"data\":\"value\"}", "msg-123", "corr-456", "env.domain.entity.event.v1",
            "entity-1", "Event.v1", KafkaEnvelopeKinds.S3Reference, signature));
        Assert.False(service.VerifySignature("{\"data\":\"value\"}", "msg-123", "corr-456", signature));
        _mockEncryptionService.Verify(service => service.Encrypt(It.IsAny<string>()), Times.Never);
    }

    [Theory]
    [InlineData("v2.primary.short")]
    [InlineData("v2.primary.///////////////////////////////////////////")]
    [InlineData("v2.primary.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.extra")]
    public void VerifySignature_WithNonCanonicalV2Mac_RejectsBeforeVerification(string signature)
    {
        var options = Options.Create(new KafkaOptions
        {
            Security = new KafkaSecuritySettings { SignatureWriteVersion = KafkaSignatureWriteVersion.HmacSha256V2 }
        });
        var service = new SignatureService(
            _mockEncryptionService.Object,
            options,
            new FixedKeyProvider("primary", Enumerable.Repeat((byte)1, 32).ToArray()));

        Assert.False(service.VerifySignature(
            "{}", "msg", "corr", "topic.v1", null, "Event.v1", KafkaEnvelopeKinds.Inline, signature));
    }

    [Fact]
    public void VerifySignature_WithRotatedVerificationKey_AcceptsHistoricalKeyId()
    {
        var oldKey = Enumerable.Repeat((byte)7, 32).ToArray();
        var newKey = Enumerable.Repeat((byte)9, 32).ToArray();
        var options = Options.Create(new KafkaOptions
        {
            Security = new KafkaSecuritySettings { SignatureWriteVersion = KafkaSignatureWriteVersion.HmacSha256V2 }
        });
        var oldService = new SignatureService(
            _mockEncryptionService.Object,
            options,
            new FixedKeyProvider("old", oldKey));
        var signature = oldService.SignMessage(
            "{}", "msg", "corr", "topic.v1", null, "Event.v1", KafkaEnvelopeKinds.Inline);
        var rotatedService = new SignatureService(
            _mockEncryptionService.Object,
            options,
            new FixedKeyProvider("new", newKey, new Dictionary<string, byte[]> { ["old"] = oldKey }));

        Assert.True(rotatedService.VerifySignature(
            "{}", "msg", "corr", "topic.v1", null, "Event.v1", KafkaEnvelopeKinds.Inline, signature));
    }

    [Fact]
    public void VerifySignature_WhenLegacyDisabled_RejectsV1()
    {
        var options = Options.Create(new KafkaOptions
        {
            Security = new KafkaSecuritySettings
            {
                SignatureWriteVersion = KafkaSignatureWriteVersion.HmacSha256V2,
                AllowLegacyV1Verification = false
            }
        });
        var keyProvider = new Mock<IKafkaSignatureKeyProvider>();
        keyProvider.Setup(provider => provider.GetKey()).Returns(new byte[32]);
        var service = new SignatureService(_mockEncryptionService.Object, options, keyProvider.Object);

        Assert.False(service.VerifySignature("{}", "msg", "corr", "encrypted_legacy"));
    }

    [Fact]
    public async Task KeyValidationHostedService_UsesCustomProviderAndRejectsShortKey()
    {
        var options = Options.Create(new KafkaOptions
        {
            Security = new KafkaSecuritySettings { SignatureWriteVersion = KafkaSignatureWriteVersion.HmacSha256V2 }
        });
        var validator = new KafkaSignatureKeyValidationHostedService(
            options,
            new FixedKeyProvider("primary", new byte[16]));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            validator.StartAsync(CancellationToken.None));
    }

    private sealed class FixedKeyProvider : IKafkaSignatureKeyProvider
    {
        private readonly string _activeKeyId;
        private readonly byte[] _activeKey;
        private readonly IReadOnlyDictionary<string, byte[]> _verificationKeys;

        public FixedKeyProvider(
            string activeKeyId,
            byte[] activeKey,
            IReadOnlyDictionary<string, byte[]>? verificationKeys = null)
        {
            _activeKeyId = activeKeyId;
            _activeKey = activeKey;
            _verificationKeys = verificationKeys ?? new Dictionary<string, byte[]>();
        }

        public byte[] GetKey() => (byte[])_activeKey.Clone();

        public string GetActiveKeyId() => _activeKeyId;

        public byte[]? GetVerificationKey(string keyId) =>
            string.Equals(keyId, _activeKeyId, StringComparison.Ordinal)
                ? (byte[])_activeKey.Clone()
                : _verificationKeys.TryGetValue(keyId, out var key) ? (byte[])key.Clone() : null;
    }
}
