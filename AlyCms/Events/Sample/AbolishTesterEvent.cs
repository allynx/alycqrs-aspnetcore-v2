using AlyCqrs.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCms.Events.Sample
{
    [Serializable]
    public class AbolishTesterEvent : Event
    {
        public AbolishTesterEvent(int version, Guid aggregateKey) : base(version, aggregateKey) { }
    }
}
