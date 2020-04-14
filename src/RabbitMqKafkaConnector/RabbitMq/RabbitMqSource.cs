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
    public class RabbitMqSource : BackgroundService
    {
        private readonly ActorSystem _system;
        private readonly RabbitMqSubscription[] _rabbitMqSubscriptions;
        private IModel _channel;
        private Channel<EventData> _eventDataChannel;

        public RabbitMqSource(IConnection connection, ActorSystem system, RabbitMqSubscription[] rabbitMqSubscriptions)
        {
            _system = system;
            _rabbitMqSubscriptions = rabbitMqSubscriptions;
            _eventDataChannel = Channel.CreateUnbounded<EventData>();
            _channel = connection.CreateModel();
        }
        

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var kafkaActor = _system.ActorSelection("/user/kafka");
            Subscribe();
            await foreach (var msg in _eventDataChannel.Reader.ReadAllAsync(stoppingToken))
            {
                kafkaActor.Tell(msg);
            }
        }

        private void Subscribe()
        {
            foreach (var subscription in _rabbitMqSubscriptions)
            {
                CreateSubscription(subscription.Topic, subscription.From);
            }
        }
        

        private void CreateSubscription(string topic, RabbitmqConfig config)
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
                _eventDataChannel.Writer.TryWrite(new EventData(topic, ByteString.FromBytes(ea.Body)));
            };
            
            _channel.BasicConsume(queue: queueName,
                autoAck: true,
                consumer: consumer);
        }
    }
}