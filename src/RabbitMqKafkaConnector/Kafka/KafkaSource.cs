using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka.IO;
using Confluent.Kafka;
using RabbitMqKafkaConnector.Bus;
using RabbitMqKafkaConnector.Configuration;

namespace RabbitMqKafkaConnector.Kafka
{
    public class KafkaSource
    {
        private ConsumerConfig _config;
        private ImmutableDictionary<string, string> _topics;
        private Channel<EventData> _channel;
        public KafkaSource(ConsumerConfig config, Channel<EventData> channel)
        {
            _config = config;
            _channel = channel;
        }

        public async Task StartHandling(Subscription[] subscriptions)
        {
            _topics = subscriptions.Aggregate(ImmutableDictionary<string, string>.Empty,
                (acc, x) => acc.Add(x.Kafka.TopicWithEnv, x.Kafka.Topic));
            using var consumer = new ConsumerBuilder<Ignore, byte[]>(_config)
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
            consumer.Subscribe(subscriptions.Select(x => x.Kafka.TopicWithEnv).ToArray());
            try
            {
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume();
                        if (_topics.TryGetValue(cr.Topic, out var tp))
                        {
                            await _channel.Writer.WriteAsync(new EventData(tp, ByteString.FromBytes(cr.Message.Value)));
                            Console.WriteLine($"Consumed message '{cr.Message.Key}' at: '{cr.TopicPartitionOffset}'.");
                        }
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