using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMqKafkaConnector.Core.Messaging.Subscribers;
using RabbitMqKafkaConnector.Infrastructure.Kafka.Messaging.Types;
using System.Linq;

namespace RabbitMqKafkaConnector.Infrastructure.Kafka.Messaging.Subscribers
{
    public class KafkaMessageSubscriber : IMessageConsumer
    {
        private IEnumerable<KafkaSubscription> _kafkaSubscriberConfigurations;
        private readonly ConsumerConfig _consumerConfig;
        private readonly ILogger<KafkaMessageSubscriber> _logger;
        private readonly Channel<KafkaEvent> _channel;
        private readonly IConsumer<Ignore, byte[]> _consumer;

        public KafkaMessageSubscriber(IEnumerable<KafkaSubscription> kafkaSubscriberConfigurations,
            ConsumerConfig consumerConfig, ILogger<KafkaMessageSubscriber> logger, Channel<KafkaEvent> channel)
        {
            _kafkaSubscriberConfigurations = kafkaSubscriberConfigurations;
            _consumerConfig = consumerConfig;
            _logger = logger;
            _channel = channel;
            _consumer = new ConsumerBuilder<Ignore, byte[]>(_consumerConfig)
                .SetErrorHandler((_, e) => _logger.LogError("Error: {Error}", e))
                .SetStatisticsHandler((_, json) => _logger.LogTrace("Statistics: {Body}", json))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    _logger.LogWarning("ConsumerId: {ConsumerId}, Assigned partitions: {Partitions}]", c.MemberId,
                        partitions);
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    _logger.LogWarning("ConsumerId: {ConsumerId}, Revoking assignment: {Partitions}]", c.MemberId,
                        partitions);
                })
                .Build();
        }

        public async Task Consume(CancellationToken cancellationToken)
        {
            const int commitPeriod = 50;
            _consumer.Subscribe(_kafkaSubscriberConfigurations.Select(x => x.Topic).ToArray());
            await Task.Yield();
            while (true)
            {
                try
                {
                    var cr = _consumer.Consume(cancellationToken);
                    await _channel.Writer.WriteAsync(new KafkaEvent(cr.Topic,
                        cr.Message.Value), cancellationToken);
                    
               
                    if (cr.Offset % commitPeriod == 0)
                    {
                        try
                        {
                            _consumer.Commit(cr);
                        }
                        catch (KafkaException e)
                        {
                            _logger.LogError(e, "Commit error");
                        }
                    }
                }
                catch (ConsumeException e)
                {
                    if (e.Error.IsFatal)
                    {
                        _logger.LogCritical(e, "kafka consumer fatal exception");
                        throw;
                    }

                    _logger.LogError(e, "Consume error");
                }
                catch (OperationCanceledException)
                {
                    _consumer.Close();
                    _channel.Writer.Complete();
                    throw;
                }
            }
        }

        public void Dispose()
        {
            _consumer.Close(); // Commit offsets and leave the group cleanly.
            _consumer.Dispose();
        }
    }
}