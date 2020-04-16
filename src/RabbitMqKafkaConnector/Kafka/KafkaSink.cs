using System;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.IO;
using Confluent.Kafka;
using RabbitMqKafkaConnector.Bus;

namespace RabbitMqKafkaConnector.Kafka
{
    public class PublishKafkaEvent
    {
        public string Topic { get; }
        public ByteString Body { get; }

        public PublishKafkaEvent(string topic, ByteString body)
        {
            Topic = topic;
            Body = body;
        }
    }

    public class KafkaSink : ReceiveActor
    {
        private readonly ProducerConfig _config;
        private readonly IProducer<Null, byte[]> _producer;
        private readonly Configuration.Router _router;
        private ILoggingAdapter _logger = Context.GetLogger();

        public KafkaSink(ProducerConfig config, Configuration.Router router)
        {
            _config = config;
            _router = router;
            _producer = new ProducerBuilder<Null, byte[]>(_config)
                .SetErrorHandler((_, e) => _logger.Error($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => _logger.Info($"Statistics: {json}"))
                .Build();
            Ready();
        }

        public void Ready()
        {
            Receive<EventData>(msg =>
            {
                var cfg = _router.GetKafkaConfig(msg.Topic);
                _producer.ProduceAsync(cfg.TopicWithEnv, new Message<Null, byte[]>() {Value = msg.Body.ToArray()})
                    .PipeTo(Self);
            });

            Receive<DeliveryResult<Null,byte[]>>(msg =>
                {
                    _logger.Info("Delivered {Msg}", msg);
                });
        }
        

        protected override void PostStop()
        {
            _producer.Flush();
            base.PostStop();
        }
    }
}