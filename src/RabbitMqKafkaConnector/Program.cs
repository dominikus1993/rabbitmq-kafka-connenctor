using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Routing;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMqKafkaConnector.Configuration;
using RabbitMqKafkaConnector.Infrastructure;
using RabbitMqKafkaConnector.Kafka;
using RabbitMqKafkaConnector.RabbitMq;
using Router = RabbitMqKafkaConnector.Configuration.Router;

namespace RabbitMqKafkaConnector
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .UseLogging("rabbitmq-kafka-conenctor")
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddSingleton<IConnectionFactory>(s => new ConnectionFactory()
                        {Uri = new Uri(hostContext.Configuration.GetConnectionString("RabbitMq"))});
                    services.AddSingleton<IConnection>(s => s.GetService<IConnectionFactory>().CreateConnection());
                    services.Configure<ServiceConfig>(hostContext.Configuration.GetSection("Service"));
                    services.AddSingleton<Router>(sp => Router.Create(hostContext.Configuration.GetSection("Service").Get<ServiceConfig>()));

                    services.AddSingleton<ProducerConfig>(x => new ProducerConfig {BootstrapServers = "localhost:9092"});
                    services.AddSingleton<ConsumerConfig>(x => new ConsumerConfig
                    { 
                        GroupId = "test-consumer-group",
                        BootstrapServers = "localhost:9092",
                        AutoOffsetReset = AutoOffsetReset.Earliest
                    });
                    services.AddSingleton<ActorSystem>(sp =>
                    {
                        var system = ActorSystem.Create("rabbitmq-kafka-connector", ConfigurationFactory.ParseString(File.ReadAllText("./akka.hocon")));

                        var rabbit = system.ActorOf(Props
                            .Create(() => new RabbitMqSink(sp.GetService<IConnection>(),
                                sp.GetService<Router>())).WithRouter(new RoundRobinPool(4)), "rabbit");
                        var kafka = system.ActorOf(Props
                            .Create(() => new KafkaSink(sp.GetService<ProducerConfig>(), sp.GetService<Router>()))
                            .WithRouter(new RoundRobinPool(4)), "kafka");
                        return system;
                    });
                    services.AddHostedService<RabbitMqSource>();
                    services.AddHostedService<KafkaSource>();
                });
    }
}