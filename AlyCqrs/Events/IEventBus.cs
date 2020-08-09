using System.Threading.Tasks;

namespace AlyCqrs.Events
{
    public interface IEventBus
    {
        Task PublishAsync<T>(T evnt) where T : Event;
    }
}
