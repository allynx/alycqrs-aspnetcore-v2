using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Commands
{
    public interface ICommandHandler<T> where T : Command
    {
        Task ExecuteAsync(T command);
    }
}
