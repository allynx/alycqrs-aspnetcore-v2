using AlyCqrs.Domains;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Storage
{
    public interface IRepository<T> where T : AggregateRoot
    {
        Task<T> GetByKeyAsync(Guid aggregateKey);

        Task SaveAsync(AggregateRoot aggregate);
    }
}
