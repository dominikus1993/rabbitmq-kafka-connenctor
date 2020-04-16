namespace RabbitMqKafkaConnector.Configuration
{
    public class SerilogOptions
    {
        public SeqOptions Seq { get; set; } = new SeqOptions();
        public bool ConsoleEnabled { get; set; } = true;
        public string MinimumLevel { get; set; } = "Information";
        public string Format { get; set; } = "compact";
    }
}