using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMqKafkaConnector.Configuration;
using RabbitMqKafkaConnector.Connector;
using RabbitMqKafkaConnector.RabbitMq;

namespace RabbitMqKafkaConnector
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var connector = new RabbitMqToKafkaConnector(new ConnectionFactory() {HostName = "localhost"},
                new ProducerConfig {BootstrapServers = "localhost:9092"},
                new ServiceConfig()
                {
                    RabbitMqSubscriptions = new[]
                    {
                        new RabbitMqSubscription()
                        {
                            From = new RabbitmqConfig() {Exchange = "Test", Queue = "test", Topic = "d.p"},
                            To = new KafkaConfig() {Topic = "test"},
                            Topic = "test"
                        },
                    }
                });

            await connector.Connect();
        }
    }
}