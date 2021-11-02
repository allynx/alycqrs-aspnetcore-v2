using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Events
{
    public class DefaultEventBus : IEventBus
    {
        private readonly IEventHandlerFactory _factory;

        public DefaultEventBus(IEventHandlerFactory factory) {
            _factory = factory;
        }
        public async Task PublishAsync<T>(T evnt) where T : Event
        {
            IEventHandler<T> handler = _factory.GetHandler<T>();
            await handler.HandleAsync(evnt);
        }
    }
}
