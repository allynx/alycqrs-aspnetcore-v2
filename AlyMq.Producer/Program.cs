using AlyMq.Producer.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Threading.Tasks;

namespace AlyMq.Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using (ServiceProvider serviceProvider = new ServiceCollection().ConfigureServices().BuildServiceProvider())
            {
                IProducerService service = serviceProvider.GetService<IProducerService>();

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
                 .AddMqProducter();
        }
    }
}

