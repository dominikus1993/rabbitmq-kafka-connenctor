using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using RabbitMQ.Client;
using RabbitMqKafkaConnector.Configuration;
using RabbitMqKafkaConnector.Kafka;
using RabbitMqKafkaConnector.RabbitMq;

namespace RabbitMqKafkaConnector.Connector
{
    public class RabbitMqToKafkaConnector
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly ProducerConfig _producerConfig;
        private readonly IConnection _connection;
        public RabbitMqToKafkaConnector(ConnectionFactory connectionFactory, ProducerConfig producerConfig)
        {
            _connectionFactory = connectionFactory;
            _producerConfig = producerConfig;
            _connection = _connectionFactory.CreateConnection();
        }

        public async Task Connect(Subscription[] subscriptions)
        {
            var tasks = subscriptions.Select(CreateRabbitToKafkaChannels).ToArray();
            await Task.WhenAll(tasks);
        }

        private async Task CreateRabbitToKafkaChannels(Subscription subscription)
        {
            var rabbitMqSource = new RabbitMqSource(_connection);
            var kafkaSink = new KafkaSink(_producerConfig);
            var channel = rabbitMqSource.StartHandling(subscription.RabbitMq);
            await kafkaSink.StartProducing(subscription.Kafka, channel);
        }
    }
}