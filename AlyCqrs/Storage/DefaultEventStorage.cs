using AlyCqrs.Domains;
using AlyCqrs.Domains.Sources;
using AlyCqrs.Events;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SqlSugar;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Reflection.Metadata.Ecma335;
using System.Threading.Tasks;

namespace AlyCqrs.Storage
{
    public class DefaultEventStorage : IEventStorage
    {
        private readonly SqlSugarClient _sqlSugarClient;
        private readonly IEventBus _eventBus;
        private readonly ILogger<DefaultEventStorage> _logger;
        private readonly IMemoryCache _memoryCache;
        public DefaultEventStorage(ILogger<DefaultEventStorage> logger, IMemoryCache memoryCache, IEventBus eventBus)
        {
            _logger = logger;
            _eventBus = eventBus;
            _memoryCache = memoryCache;

            _sqlSugarClient = new SqlSugarClient(new ConnectionConfig()
            {
                ConnectionString = "Data Source=.;Initial Catalog=AllynCqrs;Integrated Security=True;Persist Security Info=False;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=False",
                DbType = DbType.SqlServer,//设置数据库类型
                IsAutoCloseConnection = true,//自动释放数据务，如果存在事务，在事务结束后释放
                InitKeyType = InitKeyType.Attribute //从实体特性中读取主键自增列信息
            });

            _sqlSugarClient.Aop.OnLogExecuting = (sql, pars) =>
            {
                _logger.LogDebug("{0}\r\n{1}\r\n{2}", "Event storage sql executing logger", sql, _sqlSugarClient.Utilities.SerializeObject(pars.ToDictionary(p => p.ParameterName, p => p.Value)));
            };

            _sqlSugarClient.Aop.OnError = (ex) =>
            {
                _logger.LogError(ex, "{0}\r\n{1}", "Event storage sql executed error logger", ex.StackTrace);
            };
        }

        public async Task<Memento> GetMementoAsync(Guid aggregateKey)
        {
            Memento memento;
            if (!_memoryCache.TryGetValue($"memento_{aggregateKey}", out memento))
            {
                memento = await _sqlSugarClient.Queryable<Memento>().Where(m => m.AggregateKey == aggregateKey).OrderBy(m => m.Version, OrderByType.Desc).FirstAsync();
                if (memento != null)
                {
                    _memoryCache.Set($"memento_{aggregateKey}", memento);
                }
            }
            return memento;
        }
        public Task<Memento> GetMementoAsync(Guid aggregateKey, int lastVersion)
        {
            throw new NotImplementedException("To do GetMementoAsync(Guid aggregateKey, int lastVersion)");
        }
        public Task<Memento> GetMementoAsync(Guid aggregateKey, DateTime lastDateTime)
        {
            throw new NotImplementedException("To do GetMementoAsync(Guid aggregateKey, DateTime lastDateTime)");
        }

        public async Task<IEnumerable<Event>> GetEventsAsync(Guid aggregateKey, int startVersion)
        {
            List<Event> events = new List<Event>();

            Event evnt;
            for (int version = startVersion;
                _memoryCache.TryGetValue($"event_{aggregateKey}_{version + 1}", out evnt);
                version++)
            {
                events.Add(evnt);
            }

            if (events.Count == 0)
            {
                List<EventStream> eventStreams = await _sqlSugarClient.Queryable<EventStream>().Where(e => e.AggregateKey == aggregateKey && e.Version > startVersion).ToListAsync();
                if (eventStreams != null)
                {
                    eventStreams.ForEach(s =>
                    {
                        evnt = s.GetEvent();
                        events.Add(evnt);
                        _memoryCache.Set($"event_{evnt.AggregateKey}_{evnt.Version}", evnt);
                    });
                }
            }

            return events;
        }
        public Task<IEnumerable<Event>> GetEventsAsync(Guid aggregateKey, DateTime startDateTime)
        {
            throw new NotImplementedException("To do GetEventsAsync(Guid aggregateKey, DateTime startDateTime)");
        }
        public Task<IEnumerable<Event>> GetEventsAsync(Guid aggregateKey, int startVersion, int endVersion)
        {
            throw new NotImplementedException("To do  GetEventsAsync(Guid aggregateKey, int startVersion, int endVersion)");
        }
        public Task<IEnumerable<Event>> GetEventsAsync(Guid aggregateKey, DateTime startDateTime, DateTime endDateTime)
        {
            throw new NotImplementedException("To do  GetEventsAsync(Guid aggregateKey, DateTime startDateTime, DateTime endDateTime)");
        }

        public async Task SaveAsync(Guid aggregateKey, int aggregateVersion, string aggregateTypeName, Queue<Event> events)
        {
            while (events.Count > 0)
            {
                Event evnt = events.Dequeue();
                Type type = evnt.GetType();
                string eventTypeName = type.FullName;
                string eventJson = JsonConvert.SerializeObject(evnt);

                EventStream es = new EventStream
                {
                    Id = Guid.NewGuid(),
                    AggregateKey = aggregateKey,
                    AggregateTypeName = aggregateTypeName,
                    Version = aggregateVersion,
                    EventTypeName = eventTypeName,
                    EventJson = eventJson,
                    CreatedOn = DateTime.Now
                };

                await _sqlSugarClient.Insertable(es).ExecuteCommandAsync();
                _memoryCache.Set($"event_{evnt.AggregateKey}_{evnt.Version}", evnt);

                await _eventBus.PublishAsync(evnt as dynamic);
            }
        }
        public async Task SaveAsync(Guid aggregateKey, int aggregateVersion, string aggregateTypeName, byte[] aggregateBinary, Queue<Event> events)
        {
            Memento memento = new Memento
            {
                Id = Guid.NewGuid(),
                AggregateKey = aggregateKey,
                Version = aggregateVersion,
                AggregateBinary = aggregateBinary,
                CreatedOn = DateTime.Now
            };

            await _sqlSugarClient.Insertable(memento).ExecuteCommandAsync();
            Memento mementoCache = _memoryCache.Get<Memento>($"memento_{aggregateKey}");
            _memoryCache.Set($"memento_{aggregateKey}", memento);

            while (events.Count > 0)
            {
                Event evnt = events.Dequeue();
                Type type = evnt.GetType();
                string eventTypeName = type.FullName;
                string eventJson = JsonConvert.SerializeObject(evnt);

                EventStream es = new EventStream
                {
                    Id = Guid.NewGuid(),
                    AggregateKey = aggregateKey,
                    AggregateTypeName = aggregateTypeName,
                    Version = aggregateVersion,
                    EventTypeName = eventTypeName,
                    EventJson = eventJson,
                    CreatedOn = DateTime.Now
                };

                await _sqlSugarClient.Insertable(es).ExecuteCommandAsync();

                await _eventBus.PublishAsync(evnt as dynamic);
            }

            if (mementoCache != null)
            {
                for (int i = mementoCache.Version; i < aggregateVersion; i++)
                {
                    _memoryCache.Remove($"event_{aggregateKey}_{i}");
                }
            }
        }
    }
}
