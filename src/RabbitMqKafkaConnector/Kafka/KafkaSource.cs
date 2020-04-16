using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.IO;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMqKafkaConnector.Bus;
using RabbitMqKafkaConnector.Configuration;
using RabbitMqKafkaConnector.Extensions;

namespace RabbitMqKafkaConnector.Kafka
{
    public class KafkaSource : BackgroundService

    {
        private readonly ActorSystem _system;
        private readonly ConsumerConfig _consumerConfig;
        private readonly KafkaSubscription[] _kafkaSubscriptions;
        private ILogger<KafkaSource> _logger;
        public KafkaSource(ActorSystem system, ConsumerConfig consumerConfig, IOptions<ServiceConfig> options, ILogger<KafkaSource> logger)
        {
            _system = system;
            _consumerConfig = consumerConfig;
            _kafkaSubscriptions = options.Value.KafkaSubscriptions;
            _logger = logger;
        }



        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var rabbitMq = _system.ActorSelection("/user/rabbit");
            using var consumer = new ConsumerBuilder<Ignore, byte[]>(_consumerConfig)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) =>  _logger.LogError($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) =>  _logger.LogInformation($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    _logger.LogInformation($"Assigned partitions: [{string.Join(", ", partitions)}]");
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    _logger.LogInformation($"Revoking assignment: [{string.Join(", ", partitions)}]");
                })
                .Build();
            
            consumer.Subscribe(_kafkaSubscriptions.Select(x => x.From.TopicWithEnv).ToArray());
            try
            {
                var prefixToBeTrimed = $"{KafkaConfig.EnvName}.";
                await Task.Yield();
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(stoppingToken);
                        rabbitMq.Tell(new EventData(cr.Topic.TrimPrefix(prefixToBeTrimed), ByteString.FromBytes(cr.Message.Value)));
                    }
                    catch (ConsumeException e)
                    {
                       _logger.LogError(e, $"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumer.Close();
            }
        }
    }
}