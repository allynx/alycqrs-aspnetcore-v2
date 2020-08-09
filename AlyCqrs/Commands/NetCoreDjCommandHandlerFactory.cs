using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Commands
{
    public class NetCoreDjCommandHandlerFactory : ICommandHandlerFactory
    {
        private readonly IServiceProvider _provider;

        public NetCoreDjCommandHandlerFactory(IServiceProvider provider)
        {
            _provider = provider;
        }

        public ICommandHandler<T> GetHandler<T>() where T : Command
        {
            return _provider.GetService<ICommandHandler<T>>();
        }
    }
}
