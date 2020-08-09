using AlyCqrs.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCms.Events.Sample
{
    [Serializable]
    public class CreateTesterEvent : Event
    {
        public string Title { get; set; }
        public bool Disable { get; set; }
        public CreateTesterEvent(string title,bool disable) : base(1, Guid.NewGuid())
        {
            Title = title;
            Disable = disable;
        }
    }
}
