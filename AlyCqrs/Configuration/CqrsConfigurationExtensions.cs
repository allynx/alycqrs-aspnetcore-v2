using AlyCqrs.Commands;
using AlyCqrs.Events;
using AlyCqrs.Query;
using AlyCqrs.Storage;
using AlyCqrs.Synhronizers;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace AlyCqrs.Configuration
{
    public static class CqrsConfigurationExtensions
    {
        public static IServiceCollection AddCqrs(this IServiceCollection services)
        {

            services.AddMemoryCache(options =>{
                    //options.SizeLimit = 2; // 缓存最大为100份,注意netcore中的缓存是没有单位的，缓存项和缓存的相对关系
                    options.CompactionPercentage = 0.2;//缓存满了时候压缩20%的优先级较低的数据
                    options.ExpirationScanFrequency = TimeSpan.FromSeconds(60); //两秒钟查找一次过期项
                })
                .AddTransient<ICommandBus, CommandBus>()
                .AddTransient<ICommandHandlerFactory, NetCoreDjCommandHandlerFactory>()
                .AddTransient<IEventBus, EventBus>()
                .AddTransient<IEventHandlerFactory, NetCoreDjEventHandlerFactory>()
                .AddTransient<IEventStorage, DefaultEventStorage>()
                .AddTransient<ISynhronizer, DefaultSynhronizer>()
                .AddTransient<IQueryService, DefaultQueryService>()
                .AddTransient(typeof(IRepository<>), typeof(DefaultRepository<>));

            return services;
        }
    }
}
