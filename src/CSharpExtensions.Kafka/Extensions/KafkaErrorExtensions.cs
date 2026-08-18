using System;
using Confluent.Kafka;
using RailwayError = CSharpExtensions.Core.Railway.Error;

namespace CSharpExtensions.Kafka.Extensions;

/// <summary>
/// Provides extension methods to map Kafka exceptions to Railway Error models.
/// </summary>
public static class KafkaErrorExtensions
{
    /// <summary>
    /// Maps a general Exception to a Kafka error if it matches Kafka exception types.
    /// </summary>
    public static RailwayError KafkaConsumeError(Exception exception)
    {
        switch (exception)
        {
            case ConsumeException consumeException:
                return KafkaConsumeError(consumeException);
            case KafkaException kafkaException:
                return KafkaError(kafkaException);
        }

        var error = new RailwayError("Kafka consumer operation failed.");
        error.AsInternalServer("Kafka.UnknownError", "An unexpected Kafka error occurred.");
        error.WithMetadata("ErrorType", exception.GetType().Name);
        error.CausedBy(exception);
        return error;
    }

    /// <summary>
    /// Maps a ConsumeException to a Railway Error containing metadata.
    /// </summary>
    public static RailwayError KafkaConsumeError(ConsumeException exception)
    {
        var error = new RailwayError("Kafka consume operation failed.");
        error.AsInternalServer("Kafka.ConsumeError", "An error occurred while consuming messages from Kafka.");
        
        error.WithMetadata("ErrorCode", exception.Error.Code.ToString());
        error.WithMetadata("IsFatal", exception.Error.IsFatal);
        error.WithMetadata("IsLocalError", exception.Error.IsLocalError);
        
        error.CausedBy(exception);
        return error;
    }

    /// <summary>
    /// Maps a KafkaException to a Railway Error.
    /// </summary>
    public static RailwayError KafkaError(KafkaException exception)
    {
        var error = new RailwayError("Kafka client operation failed.");
        error.AsInternalServer("Kafka.ClientError", "A Kafka client error occurred.");
        
        error.WithMetadata("ErrorCode", exception.Error.Code.ToString());
        error.WithMetadata("IsFatal", exception.Error.IsFatal);
        
        error.CausedBy(exception);
        return error;
    }
}
