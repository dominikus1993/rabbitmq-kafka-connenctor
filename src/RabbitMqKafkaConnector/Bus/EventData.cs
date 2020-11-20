using System;
using Akka.IO;

namespace RabbitMqKafkaConnector.Bus
{
    public class EventData
    {
        public string Topic { get; }
        public ReadOnlyMemory<byte> Body { get; }

        public EventData(string topic, ReadOnlyMemory<byte>  body)
        {
            Topic = topic;
            Body = body;
        }
    }
}