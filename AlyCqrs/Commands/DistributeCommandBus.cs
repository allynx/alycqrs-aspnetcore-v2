﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Commands
{
    public class DistributeCommandBus : ICommandBus
    {
        public Task SendAsync<T>(T command) where T : Command
        {
            throw new NotImplementedException();
        }
    }
}
