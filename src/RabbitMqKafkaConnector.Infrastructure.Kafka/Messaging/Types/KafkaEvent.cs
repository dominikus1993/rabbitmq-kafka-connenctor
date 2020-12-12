using System;
using RabbitMqKafkaConnector.Core.Messaging.Types;

namespace RabbitMqKafkaConnector.Infrastructure.Kafka.Messaging.Types
{
    public record KafkaEvent(string Topic, ReadOnlyMemory<byte> Body) : IMessage;
}