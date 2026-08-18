namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Descriptor representing a single step configuration in a stateful message enrichment composite context.
/// </summary>
public sealed class CompositeStepDescriptor
{
    /// <summary>
    /// Gets or sets the event message type.
    /// </summary>
    public Type EventType { get; set; } = null!;

    /// <summary>
    /// Gets or sets the delegate to extract the assembly key from the event payload.
    /// </summary>
    public Delegate KeySelector { get; set; } = null!;

    /// <summary>
    /// Gets or sets the delegate to map the event payload onto the composite context.
    /// </summary>
    public Delegate Enricher { get; set; } = null!;

    /// <summary>
    /// Gets or sets the type name of the event that must precede this step in ordered chains.
    /// </summary>
    public string? PredecessorEventTypeName { get; set; }
}

/// <summary>
/// Defines builder contract for unordered composite aggregation.
/// </summary>
/// <typeparam name="TComposite">The type of the composite context.</typeparam>
public interface IUnorderedCompositeBuilder<TComposite> where TComposite : class, ICompositeContext
{
    /// <summary>
    /// Configures an event to be aggregated. Arrival order is not enforced.
    /// </summary>
    IUnorderedCompositeBuilder<TComposite> With<TEvent>(
        Func<TEvent, string>? keySelector = null,
        Action<TComposite, TEvent>? enricher = null) where TEvent : class;

    /// <summary>
    /// Registers the handler for the completed composite context.
    /// </summary>
    void AddHandler<THandler>() where THandler : IMessageHandler<TComposite>;
}

/// <summary>
/// Defines builder contract for ordered composite chain aggregation.
/// </summary>
/// <typeparam name="TComposite">The type of the composite context.</typeparam>
public interface IOrderedCompositeBuilder<TComposite> where TComposite : class, ICompositeContext
{
    /// <summary>
    /// Configures the next event in the sequence. Must arrive after the previous event is registered.
    /// </summary>
    IOrderedCompositeBuilder<TComposite> FollowedBy<TEvent>(
        Func<TEvent, string>? keySelector = null,
        Action<TComposite, TEvent>? enricher = null) where TEvent : class;

    /// <summary>
    /// Registers the handler for the completed composite context.
    /// </summary>
    void AddHandler<THandler>() where THandler : IMessageHandler<TComposite>;
}

/// <summary>
/// Builder responsible for configuring stateful aggregation context, event mapping, and handlers.
/// </summary>
/// <typeparam name="TComposite">The type of the composite context.</typeparam>
public sealed class CompositeMessageBuilder<TComposite> :
    IUnorderedCompositeBuilder<TComposite>,
    IOrderedCompositeBuilder<TComposite>
    where TComposite : class, ICompositeContext
{
    /// <summary>
    /// Gets the list of steps configured for this composite aggregator.
    /// </summary>
    public List<CompositeStepDescriptor> Steps { get; } = new();

    /// <summary>
    /// Gets the registered handler type to process the finalized composite context.
    /// </summary>
    public Type? HandlerType { get; private set; }

    /// <summary>
    /// Gets a value indicating whether sequence step ordering is strictly enforced.
    /// </summary>
    public bool IsOrdered { get; private set; }

    /// <summary>
    /// Configures an event to be aggregated in any order.
    /// </summary>
    public IUnorderedCompositeBuilder<TComposite> With<TEvent>(
        Func<TEvent, string>? keySelector = null,
        Action<TComposite, TEvent>? enricher = null) where TEvent : class
    {
        IsOrdered = false;
        AddStep(keySelector, enricher, predecessor: null);
        return this;
    }

    /// <summary>
    /// Configures the first event in an ordered sequence chain.
    /// </summary>
    public IOrderedCompositeBuilder<TComposite> StartWith<TEvent>(
        Func<TEvent, string>? keySelector = null,
        Action<TComposite, TEvent>? enricher = null) where TEvent : class
    {
        IsOrdered = true;
        AddStep(keySelector, enricher, predecessor: null);
        return this;
    }

    /// <summary>
    /// Configures the next event in an ordered sequence chain.
    /// </summary>
    public IOrderedCompositeBuilder<TComposite> FollowedBy<TEvent>(
        Func<TEvent, string>? keySelector = null,
        Action<TComposite, TEvent>? enricher = null) where TEvent : class
    {
        var predecessor = Steps.LastOrDefault()?.EventType.Name;
        AddStep(keySelector, enricher, predecessor);
        return this;
    }

    /// <summary>
    /// Registers the handler for the completed composite context.
    /// </summary>
    public void AddHandler<THandler>() where THandler : IMessageHandler<TComposite>
    {
        HandlerType = typeof(THandler);
    }

    private void AddStep<TEvent>(
        Func<TEvent, string>? keySelector,
        Action<TComposite, TEvent>? enricher,
        string? predecessor) where TEvent : class
    {
        var resolvedKeySelector = keySelector ?? ResolveImplicitKeySelector<TEvent>();
        var resolvedEnricher = enricher ?? ResolveImplicitEnricher<TEvent>();

        Steps.Add(new CompositeStepDescriptor
        {
            EventType = typeof(TEvent),
            KeySelector = resolvedKeySelector,
            Enricher = resolvedEnricher,
            PredecessorEventTypeName = predecessor
        });
    }

    private Func<TEvent, string> ResolveImplicitKeySelector<TEvent>() where TEvent : class
    {
        var properties = typeof(TEvent).GetProperties();

        // 1. Marked with [AssemblyKey]
        var keyProp = properties.FirstOrDefault(p => p.GetCustomAttribute<AssemblyKeyAttribute>() != null);

        // 2. Named AssemblyKey, OrderId, Id (case-insensitive)
        if (keyProp == null)
        {
            keyProp = properties.FirstOrDefault(p => string.Equals(p.Name, "AssemblyKey", StringComparison.OrdinalIgnoreCase))
                ?? properties.FirstOrDefault(p => string.Equals(p.Name, "OrderId", StringComparison.OrdinalIgnoreCase))
                ?? properties.FirstOrDefault(p => string.Equals(p.Name, "Id", StringComparison.OrdinalIgnoreCase));
        }

        if (keyProp == null)
        {
            throw new InvalidOperationException(
                $"Cannot resolve implicit assembly key for event type '{typeof(TEvent).FullName}'. " +
                "Please mark a key property with [AssemblyKey], name it AssemblyKey/OrderId/Id, or specify keySelector explicitly.");
        }

        return evt => keyProp.GetValue(evt)?.ToString() ?? throw new InvalidOperationException($"Assembly key property '{keyProp.Name}' returned null for event '{typeof(TEvent).FullName}'.");
    }

    private Action<TComposite, TEvent> ResolveImplicitEnricher<TEvent>() where TEvent : class
    {
        var compositeProps = typeof(TComposite).GetProperties();

        // Find writable property of type TEvent or matching name
        var prop = compositeProps.FirstOrDefault(p => p.PropertyType == typeof(TEvent) && p.CanWrite)
            ?? compositeProps.FirstOrDefault(p => string.Equals(p.Name, typeof(TEvent).Name, StringComparison.OrdinalIgnoreCase) && p.CanWrite);

        if (prop == null)
        {
            throw new InvalidOperationException(
                $"Cannot resolve implicit enricher for event type '{typeof(TEvent).FullName}' on composite '{typeof(TComposite).FullName}'. " +
                "No writable property of matching type or name was found. Please specify enricher explicitly.");
        }

        return (composite, evt) => prop.SetValue(composite, evt);
    }
}
