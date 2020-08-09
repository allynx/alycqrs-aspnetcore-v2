using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyMq.Consumer.Configuration
{
    public static class ConsumerConfigurationExtensions
    {
        public static IServiceCollection AddMqConsumer(this IServiceCollection services)
        {
            services.AddTransient<IConsumerService, DefaultConsumerService>();

            return services;
        }
    }
}
