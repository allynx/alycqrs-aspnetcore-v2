using AlyCqrs.Events;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace AlyCqrs.Domains.Sources
{
    public class EventStream
    {
        public Guid Id { get; internal set; }

        public Guid AggregateKey { get; internal set; }

        public string AggregateTypeName { get; internal set; }

        public string EventTypeName { get; internal set; }

        public int Version { get; internal set; }

        public DateTime CreatedOn { get; internal set; }

        public string EventJson { get; internal set; }

        public Event GetEvent()
        {
            return JsonConvert.DeserializeObject(this.EventJson, Assembly.Load(this.EventTypeName.Substring(0, this.EventTypeName.IndexOf('.'))).GetType(this.EventTypeName)) as Event;
        }
    }
}
