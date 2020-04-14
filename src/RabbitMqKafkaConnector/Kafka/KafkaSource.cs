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
using RabbitMqKafkaConnector.Bus;
using RabbitMqKafkaConnector.Configuration;

namespace RabbitMqKafkaConnector.Kafka
{
    public class KafkaSource : BackgroundService

    {
        private readonly ActorSystem _system;
        private readonly ConsumerConfig _consumerConfig;
        private readonly KafkaSubscription[] _kafkaSubscriptions;
        private Channel<EventData> _eventDataChannel;
        public KafkaSource(ActorSystem system, ConsumerConfig consumerConfig, KafkaSubscription[] kafkaSubscriptions)
        {
            _system = system;
            _consumerConfig = consumerConfig;
            _kafkaSubscriptions = kafkaSubscriptions;
            _eventDataChannel = Channel.CreateUnbounded<EventData>();
        }



        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var rabbitMq = _system.ActorSelection("/user/rabbit");
            using var consumer = new ConsumerBuilder<Ignore, byte[]>(_consumerConfig)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]");
                })
                .Build();
            consumer.Subscribe(_kafkaSubscriptions.Select(x => x.From.TopicWithEnv).ToArray());
            try
            {
                await Task.Yield();
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(stoppingToken);
                        rabbitMq.Tell(new EventData(cr.Topic.(KafkaConfig.EnvName), ByteString.FromBytes(cr.Message.Value)));
                        Console.WriteLine($"Consumed message '{cr.Message.Key}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
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