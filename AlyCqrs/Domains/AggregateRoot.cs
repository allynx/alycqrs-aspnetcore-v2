using AlyCqrs.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCqrs.Domains
{
    [Serializable]
    public class AggregateRoot : IAggregateRoot
    {
        private readonly Queue<Event> _uncommittedEvents;

        public Guid Id { get; protected set; }

        public int Version { get; protected set; }

        public AggregateRoot()
        {
            _uncommittedEvents = new Queue<Event>();
        }

        public Queue<Event> GetUnCommittedEvent()
        {
            Queue<Event> events = new Queue<Event>();
            while (_uncommittedEvents.Count > 0)
            {
                events.Enqueue(_uncommittedEvents.Dequeue());
            }
            return events;
        }

        public void ApplyEvent(Event evnt)
        {
            (this as dynamic).Handler(evnt as dynamic);
            _uncommittedEvents.Enqueue(evnt);
        }
        public void ReplayEvent(Event evnt)
        {
            (this as dynamic).Handler(evnt as dynamic);
        }

        public void ReplayEvents(IEnumerable<Event> history)
        {
            foreach (Event evnt in history) { (this as dynamic).Handler(evnt as dynamic); }
        }
    }
}
