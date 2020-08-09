using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCqrs.Commands
{
    [Serializable]
    public class Command : ICommand
    {
        public Guid Id { get; }

        public Command(Guid id)
        {
            Id = id;
        }
    }
}
