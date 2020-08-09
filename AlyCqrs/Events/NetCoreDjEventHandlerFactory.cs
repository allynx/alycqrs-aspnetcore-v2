using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCqrs.Events
{
    public class NetCoreDjEventHandlerFactory : IEventHandlerFactory
    {
        private readonly IServiceProvider _provider;

        public NetCoreDjEventHandlerFactory(IServiceProvider provider)
        {
            _provider = provider;
        }

        public IEventHandler<T> GetHandler<T>() where T : Event
        {
            return _provider.GetService<IEventHandler<T>>();
        }
    }
}
