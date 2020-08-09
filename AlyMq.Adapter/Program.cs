using AlyMq.Adapter.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using System;
using System.IO;
using System.Threading.Tasks;

namespace AlyMq.Adapter
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using (ServiceProvider serviceProvider = new ServiceCollection().ConfigureServices().BuildServiceProvider())
            {
                IAdapterService service = serviceProvider.GetService<IAdapterService>();

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
                 .AddMqAdapter();
        }
    }
}
