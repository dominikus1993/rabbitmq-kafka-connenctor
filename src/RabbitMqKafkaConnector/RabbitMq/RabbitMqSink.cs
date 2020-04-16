using System;
using System.Collections.Immutable;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.IO;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing;
using RabbitMqKafkaConnector.Bus;

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
        private readonly Configuration.Router _router;

        public RabbitMqSink(IConnection connection, Configuration.Router router)
        {
            _connection = connection;
            _router = router;
            _channel = connection.CreateModel();
            Ready();
        }


        public void Ready()
        {
            Receive<EventData>(msg =>
            {
                var rabbit = _router.GetRabbitConfig(msg.Topic);
                _channel.ExchangeDeclare(exchange: rabbit.Exchange,
                    type: "topic", true);
                _channel.BasicPublish(exchange: rabbit.Exchange,
                    routingKey: rabbit.Topic,
                    basicProperties: null,
                    body: msg.Body.ToArray());
            });
        }
    }
}