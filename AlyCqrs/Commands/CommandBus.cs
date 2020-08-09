using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Commands
{
    public class CommandBus : ICommandBus
    {
        private readonly ICommandHandlerFactory _factory;

        public CommandBus(ICommandHandlerFactory factory)
        {
            _factory = factory;
        }

        public async Task SendAsync<T>(T command) where T : Command
        {
            ICommandHandler<T> handler = _factory.GetHandler<T>();
            await handler.ExecuteAsync(command);
        }
    }
}
