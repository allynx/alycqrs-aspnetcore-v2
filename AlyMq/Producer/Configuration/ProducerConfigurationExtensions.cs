using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyMq.Producer.Configuration
{
    public static class ProducerConfigurationExtensions
    {
        public static IServiceCollection AddMqProducter(this IServiceCollection services)
        {
            IConfiguration config = new ConfigurationBuilder()
              .AddJsonFile("producerconfig.json", true, true)
              .Build();

            config.Bind("ProducerConfig", ProducerConfig.Create());

           return services.AddTransient<IProducerService, DefaultProducerService>();
        }
    }
}
