using AlyCms.Events.Sample;
using AlyCqrs.Events;
using AlyCqrs.Synhronizers;
using SqlSugar;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCms.EventHandlers.Sample
{
    public class CreateTesterEventHandler : IEventHandler<CreateTesterEvent>
    {
        private readonly ISynhronizer _synhronizer;

        public CreateTesterEventHandler(ISynhronizer synhronizer) {
            _synhronizer = synhronizer;
        }

        public async Task HandleAsync(CreateTesterEvent evnt)
        {
            await _synhronizer.ExceuteAsync(@"insert into [tester]([id],[title],[disable])values(@id,@title,@disable)", new { id = evnt.AggregateKey, title = evnt.Title, disable = evnt.Disable });
        }
    }
}
