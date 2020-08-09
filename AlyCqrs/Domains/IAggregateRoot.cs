using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCqrs.Domains
{
    public interface IAggregateRoot
    {
        Guid Id { get; }

        int Version { get; }
    }
}
