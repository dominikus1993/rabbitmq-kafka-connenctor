using System;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka.IO;
using Confluent.Kafka;
using RabbitMqKafkaConnector.Bus;

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

    public class KafkaSink
    {
        private ProducerConfig _config;
        private IProducer<Null, byte[]> _producer;
        private Configuration.Router _router;

        public KafkaSink(ProducerConfig config, Configuration.Router router)
        {
            _config = config;
            _router = router;
            _producer = new ProducerBuilder<Null, byte[]>(_config).Build();
        }

        public async Task Ready(ChannelReader<EventData> channelReader)
        {
            await foreach (var msg in channelReader.ReadAllAsync())
            {
                var cfg = _router.GetKafkaConfig(msg.Topic);
                await _producer.ProduceAsync(cfg.TopicWithEnv, new Message<Null, byte[]>() {Value = msg.Body.ToArray()});
                Console.WriteLine("Produced");
            }
        }
    }
}