using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyMq.Adapter.Configuration
{
    public static class AdapterConfigurationExtensions
    {
        public static IServiceCollection AddMqAdapter(this IServiceCollection services)
        {
            IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile("adapterconfig.json", true, true)
                .Build();

            config.Bind("AdapterConfig", AdapterConfig.Create());

            return services.AddTransient<IAdapterService, DefaultAdapterService>();
        }
    }
}
