// using System.Linq;
// using System.Threading.Channels;
// using System.Threading.Tasks;
// using Confluent.Kafka;
// using RabbitMQ.Client;
// using RabbitMqKafkaConnector.Bus;
// using RabbitMqKafkaConnector.Configuration;
// using RabbitMqKafkaConnector.Kafka;
// using RabbitMqKafkaConnector.RabbitMq;
//
// namespace RabbitMqKafkaConnector.Connector
// {
//     public class KafkaToRabbitMqConnector
//     {
//         private readonly ConnectionFactory _connectionFactory;
//         private readonly ConsumerConfig _consumerConfig;
//         private readonly IConnection _connection;
//         public KafkaToRabbitMqConnector(ConnectionFactory connectionFactory, ConsumerConfig consumerConfig)
//         {
//             _connectionFactory = connectionFactory;
//             _consumerConfig = consumerConfig;
//             _connection = _connectionFactory.CreateConnection();
//         }
//
//         public async Task Connect(Subscription[] subscriptions)
//         {
//             var channel = Channel.CreateUnbounded<EventData>();
//             var source = new KafkaSource(_consumerConfig, channel);
//             
//             await source.StartHandling(subscriptions);
//         }
//
//         private async Task CreateRabbitToKafkaChannels(Subscription subscription)
//         {
//             var rabbitMqSource = new RabbitMqSource(_connection);
//             var channel = rabbitMqSource.StartHandling(subscription.RabbitMq);
//            // await kafkaSink.StartProducing(subscription.Kafka, channel);
//         }
//     }
// }