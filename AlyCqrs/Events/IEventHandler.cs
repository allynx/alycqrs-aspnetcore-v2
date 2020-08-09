using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Events
{
    public interface IEventHandler<T> where T : Event
    {
        Task HandleAsync(T evnt);
    }
}
