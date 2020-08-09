using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCqrs.Domains.Sources
{
    public class Memento
    {
        public Guid Id { get; internal set; }

        public Guid AggregateKey { get; internal set; }

        public int Version { get; internal set; }

        public DateTime CreatedOn { get; internal set; }

        public byte[] AggregateBinary { get; internal set; }
    }
}
