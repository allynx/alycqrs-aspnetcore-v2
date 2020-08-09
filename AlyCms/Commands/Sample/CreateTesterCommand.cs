using AlyCqrs.Commands;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCms.Commands.Sample
{
    public class CreateTesterCommand : Command
    {
        public string Title { get; }

        public bool Disable { get; }

        public CreateTesterCommand(string title,bool disable) : base(Guid.NewGuid())
        {
            Title = title;
            Disable = disable;
        }
    }
}
