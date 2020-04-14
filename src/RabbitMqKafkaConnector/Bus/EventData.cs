using Akka.IO;

namespace RabbitMqKafkaConnector.Bus
{
    public class EventData
    {
        public string Topic { get; }
        public ByteString Body { get; }

        public EventData(string topic, ByteString body)
        {
            Topic = topic;
            Body = body;
        }
    }
}