using System;

namespace RabbitMqKafkaConnector.Core.Messaging.Types
{
    public interface IMessage
    {
        string Topic { get; init; }
        ReadOnlyMemory<byte> Body { get; init; }
    }
}