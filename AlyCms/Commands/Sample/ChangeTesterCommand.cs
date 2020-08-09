using AlyCqrs.Commands;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCms.Commands.Sample
{
    public class ChangeTesterCommand : Command
    {
        public string Title { get; }
        public bool Disable { get; }
        public ChangeTesterCommand(Guid id, string title, bool disable) : base(id)
        {
            Title = title;
            Disable = disable;
        }
    }
}
