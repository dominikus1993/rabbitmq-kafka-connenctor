using System;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;
using RabbitMqKafkaConnector.Bus;
using RabbitMqKafkaConnector.Configuration;

namespace RabbitMqKafkaConnector.Kafka
{
    public class KafkaSink
    {
        private ProducerConfig _config;

        public KafkaSink(ProducerConfig config)
        {
            _config = config;
        }

        public async Task StartProducing(KafkaSubscription subscription, Channel<EventData> channel)
        {
            using var p = new ProducerBuilder<Null, byte[]>(_config).Build();
            await foreach (var msg in channel.Reader.ReadAllAsync())
            {
                try
                {
                    var dr = await p.ProduceAsync(subscription.TopicWithEnv, new Message<Null, byte[]> { Value = msg.Body.ToArray() });
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    throw;
                }
            }
        }
    }
}