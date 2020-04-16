using System;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.IO;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqKafkaConnector.Bus;
using RabbitMqKafkaConnector.Configuration;

namespace RabbitMqKafkaConnector.RabbitMq
{
    public class RabbitMqSource : BackgroundService

    {
        private readonly RabbitMqSubscription[] _rabbitMqSubscriptions;
        private readonly IModel _channel;
        private readonly ActorSystem _actorSystem;

        public RabbitMqSource(IConnection connection, IOptions<ServiceConfig> config,
            ActorSystem actorSystem)
        {
            _rabbitMqSubscriptions = config.Value.RabbitMqSubscriptions;
            _actorSystem = actorSystem;
            _channel = connection.CreateModel();
        }

        private void CreateSubscription(string topic, RabbitmqConfig config, ActorSelection actor)
        {
            _channel.ExchangeDeclare(exchange: config.Exchange, type: "topic", true, false);
            var queueName = $"{config.Exchange}-{config.Queue}";
            
            var q = _channel.QueueDeclare(queueName, true, false, false);
            _channel.QueueBind(queue: q.QueueName,
                exchange: config.Exchange,
                routingKey: config.Topic);


            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += (_, ea) => actor.Tell(new EventData(topic, ByteString.FromBytes(ea.Body)));
            _channel.BasicConsume(queue: queueName,
                autoAck: true,
                consumer: consumer);
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var kafka = _actorSystem.ActorSelection("/user/kafka");

            foreach (var subscription in _rabbitMqSubscriptions)
            {
                CreateSubscription(subscription.Topic, subscription.From, kafka);
            }

            return Task.CompletedTask;
        }
    }
}