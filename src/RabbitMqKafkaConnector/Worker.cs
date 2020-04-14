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
                new ProducerConfig {BootstrapServers = "localhost:9092"});

            await connector.Connect(new[]
            {
                new Subscription()
                {
                    Kafka = new KafkaSubscription() {Topic = "test"},
                    RabbitMq = new RabbitmqSubscription() {Exchange = "Test", Queue = "test", Topic = "d.test"}
                },
                new Subscription()
                {
                    Kafka = new KafkaSubscription() {Topic = "test2"},
                    RabbitMq = new RabbitmqSubscription() {Exchange = "Test2", Queue = "test2", Topic = "d.test.2"}
                },
            });
        }
    }
}