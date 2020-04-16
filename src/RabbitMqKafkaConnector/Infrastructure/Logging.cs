using System;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using RabbitMqKafkaConnector.Configuration;
using Serilog;
using Serilog.Events;
using Serilog.Exceptions;
using Serilog.Formatting.Compact;
using Serilog.Formatting.Elasticsearch;
using Serilog.Sinks.SystemConsole.Themes;

namespace RabbitMqKafkaConnector.Infrastructure
{
    public static class Logging
    {
        public static IHostBuilder UseLogging(this IHostBuilder hostBuilder, string applicationName = null)
        {
            string appName = applicationName ?? Assembly.GetExecutingAssembly().FullName;
            return hostBuilder.UseSerilog(((context, configuration) =>
            {
                var serilogOptions = context.Configuration.GetSection("Serilog").Get<SerilogOptions>();
                if (!Enum.TryParse<LogEventLevel>(serilogOptions.MinimumLevel, true, out var level))
                {
                    level = LogEventLevel.Information;
                }

                var conf = configuration
                    .MinimumLevel.Is(level)
                    .Enrich.FromLogContext()
                    .Enrich.WithProperty("Environment", context.HostingEnvironment.EnvironmentName)
                    .Enrich.WithProperty("ApplicationName", appName)
                    .Enrich.WithDemystifiedStackTraces()
                    .Enrich.WithEnvironmentUserName()
                    .Enrich.WithProcessId()
                    .Enrich.WithProcessName()
                    .Enrich.WithThreadId()
                    .Enrich.WithExceptionDetails()
                    .Enrich.WithCorrelationId()
                    .Enrich.WithCorrelationIdHeader();

                conf.WriteTo.Async((logger) =>
                {
                    if (serilogOptions.Seq.Enabled)
                    {
                        logger.Seq(serilogOptions.Seq.Url);
                    }
                    
                    if (serilogOptions.ConsoleEnabled)
                    {
                        if (serilogOptions.Format.ToLower() == "elasticsearch")
                        {
                            logger.Console(new ElasticsearchJsonFormatter());
                        }
                        else if (serilogOptions.Format.ToLower() == "compact")
                        {
                            logger.Console(new CompactJsonFormatter());
                        }
                        else if (serilogOptions.Format.ToLower() == "colored")
                        {
                            logger.Console(theme: AnsiConsoleTheme.Code);
                        }
                    }

                    logger.Trace();
                });
            }));
        }
    }
}