using System;
using System.Text;
using System.Threading.Channels;
using Akka.IO;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqKafkaConnector.Bus;
using RabbitMqKafkaConnector.Configuration;

namespace RabbitMqKafkaConnector.RabbitMq
{
    public class RabbitMqSource
    {
        private IModel _channel;
        
        public RabbitMqSource(IConnection connection)
        {
            _channel = connection.CreateModel();
        }

        public Channel<EventData> StartHandling(RabbitmqSubscription subscription)
        {
            var eventChannel = Channel.CreateUnbounded<EventData>();

            _channel.ExchangeDeclare(exchange:subscription.Exchange, type: "topic");
            var queueName = $"{subscription.Exchange}-{subscription.Queue}";


            var q = _channel.QueueDeclare(queueName, true);
            _channel.QueueBind(queue: q.QueueName,
                exchange: subscription.Exchange,
                routingKey: subscription.Topic);


            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += (model, ea) =>
            {
                eventChannel.Writer.TryWrite(new EventData(ea.RoutingKey, ByteString.FromBytes(ea.Body)));
            };
            consumer.Shutdown += (sender, args) =>
            {
                eventChannel.Writer.Complete();
            };
            
            _channel.BasicConsume(queue: queueName,
                autoAck: true,
                consumer: consumer);

            return eventChannel;
        }
        
    }
}