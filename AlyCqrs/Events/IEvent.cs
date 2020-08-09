using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCqrs.Events
{
    public interface IEvent
    {
        Guid Id { get; }
    }
}
