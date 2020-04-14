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
        private readonly ServiceConfig _config;
        private readonly IConnection _connection;
        public RabbitMqToKafkaConnector(ConnectionFactory connectionFactory, ProducerConfig producerConfig, ServiceConfig config)
        {
            _connectionFactory = connectionFactory;
            _producerConfig = producerConfig;
            _config = config;
            _connection = _connectionFactory.CreateConnection();
        }

        public async Task Connect()
        {
            var router = Configuration.Router.Create(_config);
            var rabbitMqSource = new RabbitMqSource(_connection, _config.RabbitMqSubscriptions);
            var kafkaSink = new KafkaSink(_producerConfig, router);
            var channel = rabbitMqSource.Start();
            await kafkaSink.Ready(channel);
        }
        
    }
}