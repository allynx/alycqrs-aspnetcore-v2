using AlyMq.Broker.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Threading.Tasks;

namespace AlyMq.Broker
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using (ServiceProvider serviceProvider = new ServiceCollection().ConfigureServices().BuildServiceProvider())
            {
                IBrokerService service = serviceProvider.GetService<IBrokerService>();

                await service.Start();

                Console.Read();
            }
        }


    }
    internal static class Startup
    {
        internal static IServiceCollection ConfigureServices(this ServiceCollection services)
        {
            return services
                 .AddAlyMq()
                 .AddMqBroker();
        }
    }
}
