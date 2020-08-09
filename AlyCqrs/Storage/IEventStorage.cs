using AlyCqrs.Domains;
using AlyCqrs.Domains.Sources;
using AlyCqrs.Events;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Storage
{
    public interface IEventStorage
    {
        Task<Memento> GetMementoAsync(Guid aggregateKey);
        Task<Memento> GetMementoAsync(Guid aggregateKey, int lastVersion);
        Task<Memento> GetMementoAsync(Guid aggregateKey, DateTime lastDateTime);

        Task<IEnumerable<Event>> GetEventsAsync(Guid aggregateKey, int startVersion);
        Task<IEnumerable<Event>> GetEventsAsync(Guid aggregateKey, DateTime startDateTime);
        Task<IEnumerable<Event>> GetEventsAsync(Guid aggregateKey, int startVersion,int endVersion);
        Task<IEnumerable<Event>> GetEventsAsync(Guid aggregateKey, DateTime startDateTime,DateTime endDateTime);

        Task SaveAsync(Guid aggregateKey, int aggregateVersion, string aggregateTypeName, Queue<Event> events);
        Task SaveAsync(Guid aggregateKey, int aggregateVersion, string aggregateTypeName, byte[] aggregateBinary, Queue<Event> events);
    }
}
