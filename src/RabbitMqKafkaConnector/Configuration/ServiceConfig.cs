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
        public KafkaSubscription[] KafkaSubscriptions { get; set; }
        public RabbitMqSubscription[] RabbitMqSubscriptions { get; set; }
    }
}