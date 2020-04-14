using System;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.IO;
using Confluent.Kafka;
using RabbitMqKafkaConnector.Bus;
using RabbitMqKafkaConnector.Configuration;

namespace RabbitMqKafkaConnector.Kafka
{
    public class PublishKafkaEvent
    {
        public string Topic { get; }
        public ByteString Body { get; }

        public PublishKafkaEvent(string topic, ByteString body)
        {
            Topic = topic;
            Body = body;
        }
    }

    public class KafkaSink : ReceiveActor
    {
        private ProducerConfig _config;
        private IProducer<Null, byte[]> _producer;

        public KafkaSink(ProducerConfig config)
        {
            _config = config;
            _producer = new ProducerBuilder<Null, byte[]>(_config).Build();
            Ready();
        }

        public void Ready()
        {
            Receive<PublishKafkaEvent>(msg =>
            {
                _producer.Produce(msg.Topic, new Message<Null, byte[]>() {Value = msg.Body.ToArray()});
            });
        }
    }
}