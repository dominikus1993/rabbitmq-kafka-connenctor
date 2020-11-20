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
using RabbitMQ.Client;
using RabbitMqKafkaConnector.Bus;
using RabbitMqKafkaConnector.Configuration;
using RabbitMqKafkaConnector.Extensions;

namespace RabbitMqKafkaConnector.Kafka
{
    public class KafkaSource : BackgroundService

    {
        private readonly Router _router;
        private readonly ConsumerConfig _consumerConfig;
        private readonly KafkaSubscription[] _kafkaSubscriptions;
        private ILogger<KafkaSource> _logger;
        private ICon _connection;

        public KafkaSource( ConsumerConfig consumerConfig, IOptions<ServiceConfig> options,
            ILogger<KafkaSource> logger, IConnection connection, Router router)
        {
            _consumerConfig = consumerConfig;
            _kafkaSubscriptions = options.Value.KafkaSubscriptions;
            _logger = logger;
            _connection = connection;
            _router = router;
        }


        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var channel = Channel.CreateUnbounded<EventData>();
            var producer = Enumerable.Range(1, Environment.ProcessorCount)
                .Select(_ => Produce(
                    channel.Reader))
                .ToArray();
            var c = _kafkaSubscriptions.Select(x => x.From.TopicWithEnv).Select(async topic => await Task.Factory.StartNew(async () =>
            {
                await Consume(topic, channel.Writer);
            }, TaskCreationOptions.LongRunning));

            await Task.WhenAll(c.Select(x => x.Unwrap()));
            await Task.WhenAll(producer);
        }

        public async Task Consume(string topic, ChannelWriter<EventData> writer)
        {
            using var consumer = new ConsumerBuilder<Ignore, byte[]>(_consumerConfig)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => _logger.LogError($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => _logger.LogInformation($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    _logger.LogInformation($"Assigned partitions: [{string.Join(", ", partitions)}]");
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    _logger.LogError($"Revoking assignment: [{string.Join(", ", partitions)}]");
                })
                .Build();

            consumer.Subscribe(topic);
            try
            {
                var prefixToBeTrimmed = $"{KafkaConfig.EnvName}.";
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume();
                        await writer.WriteAsync(new EventData(cr.Topic.TrimPrefix(prefixToBeTrimmed),
                            cr.Message.Value));
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
        
        public async Task Produce(ChannelReader<EventData> data)
        {
            await foreach (var msg in data.ReadAllAsync())
            {
                Publish(msg);
            }
        }

        public void Publish(EventData msg)
        {
            try
            {
                var rabbit = _router.GetRabbitConfig(msg.Topic);
                _channel.ExchangeDeclare(exchange: rabbit.Exchange,
                    type: "topic", true);
                _channel.BasicPublish(exchange: rabbit.Exchange,
                    routingKey: rabbit.Topic,
                    basicProperties: null,
                    body: msg.Body.ToArray());
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error when trying publish rabbitmq message");
            }
        }
    }
}