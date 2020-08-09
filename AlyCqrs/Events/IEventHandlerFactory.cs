using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCqrs.Events
{
    public interface IEventHandlerFactory
    {
        IEventHandler<T> GetHandler<T>() where T : Event;
    }
}
