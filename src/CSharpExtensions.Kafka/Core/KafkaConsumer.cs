namespace CSharpExtensions.Kafka.Core;

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;

/// <summary>
/// Channel-based implementation of <see cref="IKafkaConsumer{TMessage}"/>.
/// Bridges the internal Kafka consumer loop to user code via a bounded channel.
/// </summary>
/// <typeparam name="TMessage">The message type.</typeparam>
internal sealed class KafkaConsumer<TMessage> : IKafkaConsumer<TMessage>
    where TMessage : class
{
    private readonly Channel<ConsumeContext<TMessage>> _channel;

    internal KafkaConsumer(int capacity = 1000)
    {
        _channel = Channel.CreateBounded<ConsumeContext<TMessage>>(
            new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = true
            });
    }

    /// <summary>
    /// Writes a consume context to the internal channel. Called from the Kafka consumer loop.
    /// </summary>
    internal async ValueTask WriteAsync(ConsumeContext<TMessage> context, CancellationToken cancellationToken)
    {
        await _channel.Writer.WriteAsync(context, cancellationToken);
    }

    /// <summary>
    /// Signals that no more messages will be written (on shutdown).
    /// </summary>
    internal void Complete()
    {
        _channel.Writer.TryComplete();
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ConsumeContext<TMessage>> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var context in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return context;
        }
    }
}
