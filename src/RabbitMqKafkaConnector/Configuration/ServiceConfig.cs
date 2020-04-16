using System;
using System.Collections.Immutable;
using System.Linq;

namespace RabbitMqKafkaConnector.Configuration
{
    public class KafkaConfig
    {
        public static string EnvName = (System.Environment.GetEnvironmentVariable("ENV") ?? "PROD").ToLower();
        public string Topic { get; set; }

        public string TopicWithEnv => $"{EnvName}.{Topic}";
    }

    public class RabbitmqConfig
    {
        public string Topic { get; set; }
        public string Exchange { get; set; }
        public string Queue { get; set; }
    }

    public class KafkaSubscription
    {
        public string Topic { get; set; }
        public KafkaConfig From { get; set; }
        public RabbitmqConfig To { get; set; }
    }

    public class RabbitMqSubscription
    {
        public string Topic { get; set; }
        public KafkaConfig To { get; set; }
        public RabbitmqConfig From { get; set; }
    }

    public class ServiceConfig
    {
        public KafkaSubscription[] KafkaSubscriptions { get; set; } = Array.Empty<KafkaSubscription>();
        public RabbitMqSubscription[] RabbitMqSubscriptions { get; set; } = Array.Empty<RabbitMqSubscription>();
    }

    public class Router
    {
        private ImmutableDictionary<string, KafkaConfig> _kafkaSinkRouting;
        private ImmutableDictionary<string, RabbitmqConfig> _rabbitmqSinkRouting;
        private Router(ImmutableDictionary<string, KafkaConfig> kafkaRouting, ImmutableDictionary<string, RabbitmqConfig> rabbitmqSinkRouting)
        {
            _kafkaSinkRouting = kafkaRouting;
            _rabbitmqSinkRouting = rabbitmqSinkRouting;
        }

        public static Router Create(ServiceConfig config)
        {
            var kafkaSinkRouting =
                config.RabbitMqSubscriptions.Aggregate(ImmutableDictionary<string, KafkaConfig>.Empty,
                    (acc, subscription) => acc.Add(subscription.Topic, subscription.To));
            var rabbitSinkRouting =
                config.KafkaSubscriptions.Aggregate(ImmutableDictionary<string, RabbitmqConfig>.Empty,
                    (acc, subscription) => acc.Add(subscription.Topic, subscription.To));
            return new Router(kafkaSinkRouting, rabbitSinkRouting);
        }

        public KafkaConfig GetKafkaConfig(string topic)
        {
            return _kafkaSinkRouting.TryGetValue(topic, out var cfg) ? cfg : new KafkaConfig() { Topic = "dead-letters" };
        }

        public RabbitmqConfig GetRabbitConfig(string topic)
        {
            return _rabbitmqSinkRouting.TryGetValue(topic, out var cfg) ? cfg : new RabbitmqConfig() {  Exchange = "dead-letters", Topic = "#" };
        }
    }
}