using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCqrs.Commands
{
    public interface ICommand
    {
        Guid Id { get; }
    }
}
