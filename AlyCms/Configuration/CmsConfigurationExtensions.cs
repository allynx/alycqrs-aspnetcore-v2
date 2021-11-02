using AlyCms.CommandHandlers.Sample;
using AlyCms.Commands.Sample;
using AlyCms.EventHandlers.Sample;
using AlyCms.Events.Sample;
using AlyCms.Querys;
using AlyCqrs.Commands;
using AlyCqrs.Configuration;
using AlyCqrs.Events;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCms.Configuration
{
    public static class CmsConfigurationExtensions
    {
        public static IServiceCollection AddCms(this IServiceCollection services)
        {
            services.AddCqrs()
                .AddCqrsProducer("127.0.0.1",7071,200)
                .AddCqrsTopics(new Guid("16a6fff9-add1-225f-960f-d8ca1b972901"), 
                    new Guid("16a6fff9-add1-225f-960f-d8ca1b972902"),
                    new Guid("16a6fff9-add1-225f-960f-d8ca1b972903"))
                .AddTransient<ITesterQueryService, TesterQueryService>()
                .AddTransient<ICommandHandler<CreateTesterCommand>, CreateTesterCommandHandler>()
                .AddTransient<ICommandHandler<ChangeTesterCommand>, ChangeTesterCommandHandler>()
                .AddTransient<ICommandHandler<AbolishTesterCommand>, AbolishTesterCommandHandler>()
                .AddTransient<IEventHandler<CreateTesterEvent>, CreateTesterEventHandler>()
                .AddTransient<IEventHandler<ChangeTesterEvent>, ChangeTesterEventHandler>()
                .AddTransient<IEventHandler<AbolishTesterEvent>, AbolishTesterEventHandler>()
                .AddTransient<ITesterQueryService, TesterQueryService>();

            return services;
        }
    }
}
