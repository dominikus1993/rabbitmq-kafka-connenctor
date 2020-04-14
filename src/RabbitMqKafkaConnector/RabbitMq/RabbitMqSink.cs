using System.Threading.Channels;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMqKafkaConnector.Bus;

namespace RabbitMqKafkaConnector.RabbitMq
{
    public class RabbitMqSink
    {
        private IModel _channel;
        private Channel<EventData> _channel;
        public RabbitMqSink(IConnection connection)
        {
            _channel = connection.CreateModel();
        }

        public Task StartPublishing()
        {
            
        }
    }
}