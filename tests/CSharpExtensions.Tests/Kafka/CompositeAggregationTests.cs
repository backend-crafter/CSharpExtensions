using CSharpExtensions.Foundation.Railway;

namespace CSharpExtensions.Tests.Kafka;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using CSharpExtensions.Kafka.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using Xunit;

public sealed class CompositeAggregationTests
{
    public class TestCompositeContext : ICompositeContext
    {
        public string AssemblyKey { get; set; } = "";
        
        public TestOrderCreatedEvent? OrderCreated { get; set; }
        public TestPaymentProcessedEvent? PaymentProcessed { get; set; }
        public TestInventoryReservedEvent? InventoryReserved { get; set; }

        public bool IsReady => OrderCreated != null && PaymentProcessed != null && InventoryReserved != null;
    }

    public class TestImplicitCompositeContext : ICompositeContext
    {
        public string AssemblyKey { get; set; } = "";
        public TestOrderCreatedEvent? TestOrderCreatedEvent { get; set; }
        public bool IsReady => TestOrderCreatedEvent != null;
    }

    public class TestOrderCreatedEvent
    {
        [AssemblyKey]
        public string OrderId { get; set; } = "";
        public string CustomerName { get; set; } = "";
    }

    public class TestPaymentProcessedEvent
    {
        public string OrderId { get; set; } = "";
        public decimal Amount { get; set; }
    }

    public class TestInventoryReservedEvent
    {
        public string OrderId { get; set; } = "";
        public string WarehouseId { get; set; } = "";
    }

    public class TestEventNoKey
    {
        public string Payload { get; set; } = "";
    }

    public class TestCompositeNoProperty : ICompositeContext
    {
        public string AssemblyKey { get; set; } = "";
        public bool IsReady => false;
    }

    [Fact]
    public void CompositeMessageBuilder_ResolvesImplicitKey_WithAssemblyKeyAttribute()
    {
        var builder = new CompositeMessageBuilder<TestCompositeContext>();
        builder.With<TestOrderCreatedEvent>();
        
        var step = builder.Steps[0];
        var evt = new TestOrderCreatedEvent { OrderId = "order_123" };
        var keySelector = (Func<TestOrderCreatedEvent, string>)step.KeySelector;
        
        Assert.Equal("order_123", keySelector(evt));
    }

    [Fact]
    public void CompositeMessageBuilder_ResolvesImplicitKey_WithPropertyName()
    {
        var builder = new CompositeMessageBuilder<TestCompositeContext>();
        builder.With<TestPaymentProcessedEvent>();
        
        var step = builder.Steps[0];
        var evt = new TestPaymentProcessedEvent { OrderId = "payment_123" };
        var keySelector = (Func<TestPaymentProcessedEvent, string>)step.KeySelector;
        
        Assert.Equal("payment_123", keySelector(evt));
    }

    [Fact]
    public void CompositeMessageBuilder_ResolvesImplicitEnricher_ByMatchingType()
    {
        var builder = new CompositeMessageBuilder<TestImplicitCompositeContext>();
        builder.With<TestOrderCreatedEvent>();
        
        var step = builder.Steps[0];
        var composite = new TestImplicitCompositeContext { AssemblyKey = "key_1" };
        var evt = new TestOrderCreatedEvent { OrderId = "order_1", CustomerName = "John" };
        
        var enricher = (Action<TestImplicitCompositeContext, TestOrderCreatedEvent>)step.Enricher;
        enricher(composite, evt);
        
        Assert.Same(evt, composite.TestOrderCreatedEvent);
    }

    [Fact]
    public void CompositeMessageBuilder_ThrowsWhenImplicitKeyCannotBeResolved()
    {
        var builder = new CompositeMessageBuilder<TestCompositeContext>();
        Assert.Throws<InvalidOperationException>(() => builder.With<TestEventNoKey>());
    }

    [Fact]
    public void CompositeMessageBuilder_ThrowsWhenImplicitEnricherCannotBeResolved()
    {
        var builder = new CompositeMessageBuilder<TestCompositeNoProperty>();
        Assert.Throws<InvalidOperationException>(() => builder.With<TestOrderCreatedEvent>());
    }

    [Fact]
    public async Task UnorderedAggregation_InvokesHandlerOnlyWhenAllEventsArrive()
    {
        // Arrange
        var builder = new CompositeMessageBuilder<TestCompositeContext>();
        builder.With<TestOrderCreatedEvent>();
        builder.With<TestPaymentProcessedEvent>();
        builder.With<TestInventoryReservedEvent>();
        builder.AddHandler<IMessageHandler<TestCompositeContext>>();
        
        var registry = new CompositeMessageRegistry();
        registry.Register(builder);
        
        var redisState = new Dictionary<string, string>();
        var mockDatabase = CreateMockDatabase(redisState);

        var mockCompositeHandler = new Mock<IMessageHandler<TestCompositeContext>>();
        mockCompositeHandler.Setup(h => h.HandleAsync(It.IsAny<TestCompositeContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Success());
            
        var serviceProvider = CreateServiceProvider(registry, mockDatabase, mockCompositeHandler, configureConnectionString: true);
        var store = serviceProvider.GetRequiredService<CompositeContextStore>();
        
        var handlerOrderCreated = new CompositeEventSubscriptionHandler<TestCompositeContext, TestOrderCreatedEvent>(
            store, registry, serviceProvider, serviceProvider.GetRequiredService<IConfiguration>(),
            serviceProvider.GetRequiredService<IOptions<KafkaOptions>>(), NullLogger<CompositeEventSubscriptionHandler<TestCompositeContext, TestOrderCreatedEvent>>.Instance);
            
        var handlerPayment = new CompositeEventSubscriptionHandler<TestCompositeContext, TestPaymentProcessedEvent>(
            store, registry, serviceProvider, serviceProvider.GetRequiredService<IConfiguration>(),
            serviceProvider.GetRequiredService<IOptions<KafkaOptions>>(), NullLogger<CompositeEventSubscriptionHandler<TestCompositeContext, TestPaymentProcessedEvent>>.Instance);
            
        var handlerInventory = new CompositeEventSubscriptionHandler<TestCompositeContext, TestInventoryReservedEvent>(
            store, registry, serviceProvider, serviceProvider.GetRequiredService<IConfiguration>(),
            serviceProvider.GetRequiredService<IOptions<KafkaOptions>>(), NullLogger<CompositeEventSubscriptionHandler<TestCompositeContext, TestInventoryReservedEvent>>.Instance);

        var assemblyKey = "order_123";
        
        // 1. Dispatch OrderCreated
        var orderEvt = new TestOrderCreatedEvent { OrderId = assemblyKey, CustomerName = "Alice" };
        var res1 = await handlerOrderCreated.HandleAsync(orderEvt, CancellationToken.None);
        Assert.True(res1.IsSuccess);
        mockCompositeHandler.Verify(h => h.HandleAsync(It.IsAny<TestCompositeContext>(), It.IsAny<CancellationToken>()), Times.Never);
        
        // 2. Dispatch PaymentProcessed
        var paymentEvt = new TestPaymentProcessedEvent { OrderId = assemblyKey, Amount = 99.99m };
        var res2 = await handlerPayment.HandleAsync(paymentEvt, CancellationToken.None);
        Assert.True(res2.IsSuccess);
        mockCompositeHandler.Verify(h => h.HandleAsync(It.IsAny<TestCompositeContext>(), It.IsAny<CancellationToken>()), Times.Never);
        
        // 3. Dispatch InventoryReserved
        var inventoryEvt = new TestInventoryReservedEvent { OrderId = assemblyKey, WarehouseId = "WH-1" };
        var res3 = await handlerInventory.HandleAsync(inventoryEvt, CancellationToken.None);
        Assert.True(res3.IsSuccess);
        
        // Handler should be called exactly once
        mockCompositeHandler.Verify(h => h.HandleAsync(
            It.Is<TestCompositeContext>(c => c.AssemblyKey == assemblyKey && c.OrderCreated != null && c.PaymentProcessed != null && c.InventoryReserved != null),
            It.IsAny<CancellationToken>()), Times.Once);
            
        // State should be deleted
        Assert.Empty(redisState);
    }

    [Fact]
    public async Task OrderedSequence_DefersEvent_WhenPredecessorIsMissing()
    {
        // Arrange
        var builder = new CompositeMessageBuilder<TestCompositeContext>();
        builder.StartWith<TestOrderCreatedEvent>();
        builder.FollowedBy<TestPaymentProcessedEvent>();
        builder.FollowedBy<TestInventoryReservedEvent>();
        builder.AddHandler<IMessageHandler<TestCompositeContext>>();
        
        var registry = new CompositeMessageRegistry();
        registry.Register(builder);
        
        var redisState = new Dictionary<string, string>();
        var mockDatabase = CreateMockDatabase(redisState);

        var mockCompositeHandler = new Mock<IMessageHandler<TestCompositeContext>>();
        
        var serviceProvider = CreateServiceProvider(registry, mockDatabase, mockCompositeHandler, configureConnectionString: false);
        var store = serviceProvider.GetRequiredService<CompositeContextStore>();
        
        var handlerPayment = new CompositeEventSubscriptionHandler<TestCompositeContext, TestPaymentProcessedEvent>(
            store, registry, serviceProvider, serviceProvider.GetRequiredService<IConfiguration>(),
            serviceProvider.GetRequiredService<IOptions<KafkaOptions>>(), NullLogger<CompositeEventSubscriptionHandler<TestCompositeContext, TestPaymentProcessedEvent>>.Instance);

        var assemblyKey = "order_123";
        
        // Act: Dispatch PaymentProcessed event first (OrderCreated is missing)
        var paymentEvt = new TestPaymentProcessedEvent { OrderId = assemblyKey, Amount = 99.99m };
        var result = await handlerPayment.HandleAsync(paymentEvt, CancellationToken.None);
        
        // Assert: It should return a failure result indicating that it could not defer (since we didn't configure connection string)
        Assert.False(result.IsSuccess);
        Assert.Contains("Cannot defer event: staged jobs database connection string is not configured.", result.Error.Message);
    }

    [Fact]
    public async Task OrderedSequence_Succeeds_WhenEventsArriveInOrder()
    {
        // Arrange
        var builder = new CompositeMessageBuilder<TestCompositeContext>();
        builder.StartWith<TestOrderCreatedEvent>();
        builder.FollowedBy<TestPaymentProcessedEvent>();
        builder.FollowedBy<TestInventoryReservedEvent>();
        builder.AddHandler<IMessageHandler<TestCompositeContext>>();
        
        var registry = new CompositeMessageRegistry();
        registry.Register(builder);
        
        var redisState = new Dictionary<string, string>();
        var mockDatabase = CreateMockDatabase(redisState);

        var mockCompositeHandler = new Mock<IMessageHandler<TestCompositeContext>>();
        mockCompositeHandler.Setup(h => h.HandleAsync(It.IsAny<TestCompositeContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Success());
            
        var serviceProvider = CreateServiceProvider(registry, mockDatabase, mockCompositeHandler, configureConnectionString: true);
        var store = serviceProvider.GetRequiredService<CompositeContextStore>();
        
        var handlerOrderCreated = new CompositeEventSubscriptionHandler<TestCompositeContext, TestOrderCreatedEvent>(
            store, registry, serviceProvider, serviceProvider.GetRequiredService<IConfiguration>(),
            serviceProvider.GetRequiredService<IOptions<KafkaOptions>>(), NullLogger<CompositeEventSubscriptionHandler<TestCompositeContext, TestOrderCreatedEvent>>.Instance);
            
        var handlerPayment = new CompositeEventSubscriptionHandler<TestCompositeContext, TestPaymentProcessedEvent>(
            store, registry, serviceProvider, serviceProvider.GetRequiredService<IConfiguration>(),
            serviceProvider.GetRequiredService<IOptions<KafkaOptions>>(), NullLogger<CompositeEventSubscriptionHandler<TestCompositeContext, TestPaymentProcessedEvent>>.Instance);
            
        var handlerInventory = new CompositeEventSubscriptionHandler<TestCompositeContext, TestInventoryReservedEvent>(
            store, registry, serviceProvider, serviceProvider.GetRequiredService<IConfiguration>(),
            serviceProvider.GetRequiredService<IOptions<KafkaOptions>>(), NullLogger<CompositeEventSubscriptionHandler<TestCompositeContext, TestInventoryReservedEvent>>.Instance);

        var assemblyKey = "order_123";
        
        var orderEvt = new TestOrderCreatedEvent { OrderId = assemblyKey, CustomerName = "Alice" };
        var res1 = await handlerOrderCreated.HandleAsync(orderEvt, CancellationToken.None);
        Assert.True(res1.IsSuccess, res1.Error?.Message);

        var key = $"kafka:composite:{{{assemblyKey}}}";
        Assert.True(redisState.ContainsKey(key), "Redis state should contain key after first step");
        var savedJson = redisState[key];
        
        // 2. Dispatch PaymentProcessed (predecessor TestOrderCreatedEvent is in state, so it succeeds)
        var paymentEvt = new TestPaymentProcessedEvent { OrderId = assemblyKey, Amount = 99.99m };
        var res2 = await handlerPayment.HandleAsync(paymentEvt, CancellationToken.None);
        Assert.True(res2.IsSuccess, $"Failed on step 2. Error: {res2.Error?.Message}. Saved JSON was: {savedJson}");
        
        // 3. Dispatch InventoryReserved (predecessor TestPaymentProcessedEvent is in state, so it succeeds and triggers handler)
        var inventoryEvt = new TestInventoryReservedEvent { OrderId = assemblyKey, WarehouseId = "WH-1" };
        var res3 = await handlerInventory.HandleAsync(inventoryEvt, CancellationToken.None);
        Assert.True(res3.IsSuccess);

        mockCompositeHandler.Verify(h => h.HandleAsync(It.IsAny<TestCompositeContext>(), It.IsAny<CancellationToken>()), Times.Once);
        Assert.Empty(redisState);
    }

    private Mock<IDatabase> CreateMockDatabase(Dictionary<string, string> redisState)
    {
        var mockDatabase = new Mock<IDatabase>();
        
        mockDatabase.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey key, CommandFlags flags) => 
                redisState.TryGetValue(key!, out var val) ? (RedisValue)val : RedisValue.Null);
                
        mockDatabase.Setup(d => d.StringSetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<Expiration>(), It.IsAny<ValueCondition>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(true)
            .Callback<RedisKey, RedisValue, Expiration, ValueCondition, CommandFlags>((k, v, e, w, f) =>
            {
                redisState[k!] = v!;
            });
            
        mockDatabase.Setup(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey key, CommandFlags flags) =>
            {
                redisState.Remove(key!);
                return true;
            });
            
        return mockDatabase;
    }

    private IServiceProvider CreateServiceProvider(
        CompositeMessageRegistry registry, 
        Mock<IDatabase> mockDatabase,
        Mock<IMessageHandler<TestCompositeContext>> mockCompositeHandler,
        bool configureConnectionString)
    {
        var services = new ServiceCollection();
        
        var mockRedisResolver = new Mock<IRedisConnectionResolver>();
        var mockMultiplexer = new Mock<IConnectionMultiplexer>();
        
        mockMultiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(mockDatabase.Object);
        mockRedisResolver.Setup(r => r.Resolve(It.IsAny<string>()))
            .Returns(mockMultiplexer.Object);
            
        services.AddSingleton<IRedisConnectionResolver>(mockRedisResolver.Object);
        
        var options = new KafkaOptions
        {
            Assembly = new MessageAssemblyOptions
            {
                Provider = AssemblyProvider.Redis,
                RedisConnectionAlias = "Default",
                StaleThresholdSeconds = 300
            },
            StagedJobs = new StagedJobSettings
            {
                ConnectionStringName = "JobsConnection",
                TableSchema = "dbo"
            }
        };
        
        services.AddSingleton(Options.Create(options));
        
        var configBuilder = new ConfigurationBuilder();
        if (configureConnectionString)
        {
            configBuilder.AddInMemoryCollection(new Dictionary<string, string?>
            {
                { "ConnectionStrings:JobsConnection", "Server=localhost;Database=test;Trusted_Connection=True;" }
            });
        }
        var configuration = configBuilder.Build();
        services.AddSingleton<IConfiguration>(configuration);
        
        services.AddSingleton(registry);
        services.AddSingleton<CompositeContextStore>();
        
        // Register the custom mock handler
        services.AddScoped(sp => mockCompositeHandler.Object);
        
        return services.BuildServiceProvider();
    }
}
