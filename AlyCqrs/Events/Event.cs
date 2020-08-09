using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCqrs.Events
{
    [Serializable]
    public class Event : IEvent
    {
        public Guid Id { get; set; }

        public int Version { get; set; }

        public Guid AggregateKey { get; set; }

        public Event(int version, Guid aggregateKey)
        {
            Id = Guid.NewGuid();
            AggregateKey = aggregateKey;
            Version = version;
        }
    }
}
