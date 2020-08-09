using AlyCqrs.Commands;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCms.Commands.Sample
{
    public class AbolishTesterCommand : Command
    {
        public AbolishTesterCommand(Guid id) : base(id) { }
    }
}
