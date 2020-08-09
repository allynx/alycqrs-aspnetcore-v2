using AlyCqrs.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCms.Events.Sample
{
    [Serializable]
    public class ChangeTesterEvent : Event
    {
        public string Title { get; set; }
        public bool Disable { get; set; }
        public ChangeTesterEvent(int version, Guid aggregateKey, string title,bool disable) : base(version, aggregateKey)
        {
            Title = title;
            Disable = disable;
        }
    }
}
