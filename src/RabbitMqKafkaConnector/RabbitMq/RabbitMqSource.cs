using System;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.IO;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqKafkaConnector.Bus;
using RabbitMqKafkaConnector.Configuration;

namespace RabbitMqKafkaConnector.RabbitMq
{
    public class RabbitMqSource
    {
        private readonly RabbitMqSubscription[] _rabbitMqSubscriptions;
        private IModel _channel;

        public RabbitMqSource(IConnection connection, RabbitMqSubscription[] rabbitMqSubscriptions)
        {
            _rabbitMqSubscriptions = rabbitMqSubscriptions;
            _channel = connection.CreateModel();
        }
        

        public Channel<EventData> Start()
        {
            var channel = Channel.CreateUnbounded<EventData>();
            
            foreach (var subscription in _rabbitMqSubscriptions)
            {
                CreateSubscription(subscription.Topic, subscription.From, channel.Writer);
            }

            return channel;
        }
        
        private void CreateSubscription(string topic, RabbitmqConfig config, ChannelWriter<EventData> writer)
        {

            _channel.ExchangeDeclare(exchange:config.Exchange, type: "topic");
            var queueName = $"{config.Exchange}-{config.Queue}";


            var q = _channel.QueueDeclare(queueName, true);
            _channel.QueueBind(queue: q.QueueName,
                exchange: config.Exchange,
                routingKey: config.Topic);


            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += (model, ea) =>
            {
                writer.TryWrite(new EventData(topic, ByteString.FromBytes(ea.Body)));
            };
            
            _channel.BasicConsume(queue: queueName,
                autoAck: true,
                consumer: consumer);
        }
    }
}