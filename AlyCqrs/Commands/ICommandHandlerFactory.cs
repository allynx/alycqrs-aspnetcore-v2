using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Commands
{
    public interface ICommandHandlerFactory
    {
        ICommandHandler<T> GetHandler<T>() where T : Command;
    }
}
