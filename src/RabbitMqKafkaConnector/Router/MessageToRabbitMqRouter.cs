using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Event;
using Akka.Routing;
using RabbitMQ.Client;
using RabbitMqKafkaConnector.Bus;
using RabbitMqKafkaConnector.Configuration;
using RabbitMqKafkaConnector.RabbitMq;

namespace RabbitMqKafkaConnector.Router
{
    public class MessageToRabbitMqRouter : ReceiveActor
    {
        private readonly IConnectionFactory _factory;
        private readonly IConnection _connection;
        private ImmutableDictionary<string, RabbitmqConfig> _routing;
        private IActorRef _rabbitMqSink = ActorRefs.Nobody;

        public MessageToRabbitMqRouter(KafkaSubscription[] kafkaSubscriptions, IConnectionFactory factory)
        {
            _factory = factory;
            _connection = _factory.CreateConnection();
            _routing = kafkaSubscriptions.Aggregate(ImmutableDictionary<string, RabbitmqConfig>.Empty,
                (acc, s) => acc.Add(s.Topic, new RabbitmqConfig() {Exchange = s.To.Exchange, Topic = s.To.Topic}));
        }

        protected override void PreStart()
        {
            _rabbitMqSink = Context.ActorOf(Props.Create(() => new RabbitMqSink(_connection)).WithRouter(new RoundRobinPool(4)));
            base.PreStart();
        }

        public void Ready()
        {
            Receive<EventData>(msg =>
            {
                if (_routing.TryGetValue(msg.Topic, out var rabbitmqConfig))
                {
                    _rabbitMqSink.Tell(new PublishRabbitMqEvent(rabbitmqConfig.Exchange, rabbitmqConfig.Topic, msg.Body));
                }
                else
                {
                    Context.GetLogger().Info("Test");
                }
            });
        }
    }
}