using System;
using System.Collections.Immutable;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.IO;
using RabbitMQ.Client;

namespace RabbitMqKafkaConnector.RabbitMq
{
    public class PublishRabbitMqEvent
    {
        public string Exchange { get; }
        public string Topic { get; }
        public ByteString Body { get; }

        public PublishRabbitMqEvent(string exchange, string topic, ByteString body)
        {
            Exchange = exchange;
            Topic = topic;
            Body = body;
        }
    }

    public class RabbitMqSink : ReceiveActor
    {
        private IModel _channel;
        private IConnection _connection;

        public RabbitMqSink(IConnection connection)
        {
            _connection = connection;
            _channel = connection.CreateModel();
            Ready();
        }


        public void Ready()
        {
            Receive<PublishRabbitMqEvent>(msg =>
            {
                _channel.ExchangeDeclare(exchange: msg.Exchange,
                    type: "topic");
                _channel.BasicPublish(exchange: "topic_logs",
                    routingKey: msg.Exchange,
                    basicProperties: null,
                    body: msg.Body.ToArray());
            });
        }
    }
}