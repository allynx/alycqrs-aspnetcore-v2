using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyMq.Broker.Configuration
{
    public static class BrokerConfigurationExtensions
    {
        public static IServiceCollection AddMqBroker(this IServiceCollection services)
        {
            IConfiguration config = new ConfigurationBuilder()
               .AddJsonFile("brokerconfig.json", true, true)
               .Build();

            config.Bind("BrokerConfig", BrokerConfig.Create());

           return services
                .AddTransient<IBrokerService, DefaultBrokerService>();
        }
    }
}
