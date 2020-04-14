namespace RabbitMqKafkaConnector.Configuration
{
    public class KafkaSubscription
    {
        private static string EnvName = (System.Environment.GetEnvironmentVariable("ENV") ?? "PROD").ToLower();
        public string Topic { get; set; }

        public string TopicWithEnv => $"{EnvName}.{Topic}";
    } 
    public class RabbitmqSubscription
    {
        public string Topic { get; set; }
        public string Exchange { get; set; }
        public string Queue { get; set; }
    }  
    
    public class Subscription
    {
        public string Topic { get; set; }
        public KafkaSubscription Kafka { get; set; }
        public RabbitmqSubscription RabbitMq { get; set; }
    }

    public class ServiceConfig
    {
        public Subscription[] KafkaSubscriptions { get; set; }
        public Subscription[] RabbitMqSubscriptions { get; set; }
    }
}