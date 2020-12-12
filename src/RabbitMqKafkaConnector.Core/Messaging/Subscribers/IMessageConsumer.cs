using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMqKafkaConnector.Core.Messaging.Subscribers
{
    public interface IMessageConsumer : IDisposable
    {
        Task Consume(CancellationToken cancellationToken);
    }
}