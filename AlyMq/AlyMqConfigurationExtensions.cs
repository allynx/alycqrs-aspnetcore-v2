using AlyMq.Broker;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Serilog.Events;
using System.IO;

namespace AlyMq
{
    public static class AlyMqConfigurationExtensions
    {
        public static IServiceCollection AddAlyMq(this IServiceCollection services)
        {
            #region Serilog Configure

            LoggerConfiguration serilogConfig = new LoggerConfiguration();

            string pathFormat = Path.Combine("logs/alymq", @"{Date}.log");
            string outputTemplatePlain = @"{Timestamp:yyyy-MM-dd HH:mm:ss.fff} -{Level}-->{NewLine}{SourceContext}{NewLine}{Message:lj}{NewLine}";
            string outputTemplateDetail = @"{Timestamp:yyyy-MM-dd HH:mm:ss.fff} -{Level}-->{NewLine}{SourceContext}{NewLine}{Properties}{NewLine}{Message:lj}{NewLine}{Exception}";

            serilogConfig.MinimumLevel.Verbose()
                    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                    .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
                    .Enrich.FromLogContext()
                    .WriteTo.Debug(LogEventLevel.Verbose, outputTemplateDetail)
                    .WriteTo.Logger(cfg =>
                        cfg.Filter.ByIncludingOnly(m => m.Level <LogEventLevel.Warning)
                           .WriteTo.Console(LogEventLevel.Verbose, outputTemplatePlain)

                    )
                    .WriteTo.Logger(cfg =>
                        cfg.Filter.ByIncludingOnly(m => m.Level > LogEventLevel.Information)
                            .WriteTo.Console(LogEventLevel.Warning, outputTemplateDetail)
                    );

            #endregion

            services.AddLogging(configure => configure.AddSerilog(serilogConfig.CreateLogger()));

            return services.AddTransient<TopicComparer>();
        }
    }
}
