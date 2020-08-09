using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Events;
using Serilog.Exceptions;
using System.IO;

namespace AlyCms.Rest
{
    public static class SerilogConfigExpantion
    {
        public static IHostBuilder UseSerilog(this IHostBuilder buider)
        {
            return buider.UseSerilog((context, config) =>
            {
                string pathFormat = Path.Combine("logs", @"{Date}.log");
                string outputTemplatePlain = @"{Timestamp:yyyy-MM-dd HH:mm:ss.fff} -{Level}-->{NewLine}{SourceContext}{NewLine}{Message:lj}{NewLine}";
                string outputTemplateDetail = @"{Timestamp:yyyy-MM-dd HH:mm:ss.fff} -{Level}-->{NewLine}{SourceContext}{NewLine}{Properties}{NewLine}{Message:lj}{NewLine}{Exception}";

                config.MinimumLevel.Verbose()
                    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                    .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
                    .Enrich.FromLogContext()
                    .WriteTo.Debug(LogEventLevel.Verbose, outputTemplateDetail)
                    .WriteTo.Logger(cfg =>
                        cfg.Filter.ByIncludingOnly(m => m.Level < LogEventLevel.Warning)
                           .WriteTo.Async(c =>
                                c.RollingFile(pathFormat, LogEventLevel.Verbose, outputTemplatePlain, null, 8388608, 31, null, false, true, null)
                           )
                    )
                    .WriteTo.Logger(cfg =>
                        cfg.Filter.ByIncludingOnly(m => m.Level > LogEventLevel.Information)
                            .WriteTo.Async(c =>
                                c.RollingFile(pathFormat, LogEventLevel.Warning, outputTemplateDetail, null, 8388608, 31, null, false, true, null)
                            )
                    );
            });
        }
    }
}
