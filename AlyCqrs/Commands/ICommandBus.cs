using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Commands
{
    public interface ICommandBus
    {
        Task SendAsync<T>(T command) where T : Command;
    }
}
