using AlyCms.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;

namespace AlyCms.Rest
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers();

            //services.AddLogging(c => c.AddLog4Net("log4net.config"));

            //¿çÓòÅäÖÃ
            services.AddCors(o => {
                o.AddPolicy("AsMgrCors", p => {
                    p.WithOrigins("http://mgr.allyn.com.cn", "https://mgr.allyn.com.cn", "http://192.168.0.2:8082", "https://192.168.0.2:8082")
                    .WithMethods("GET","POST", "PUT", "DELETE")
                    .AllowAnyHeader()
                    .AllowCredentials();
                });
            });

            services.AddCms();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env,ILoggerFactory factory)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseCors("AsMgrCors");

            app.UseHttpsRedirection();

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllerRoute(
                    name: "default",
                    pattern: "{controller=Home}/{action=Index}/{id?}"
                    );

                endpoints.MapAreaControllerRoute(
                    name: "areas",
                    areaName:"areas",
                    pattern: "{area:exists}/{controller=Home}/{action=Index}/{id?}");
            });
        }
    }
}
