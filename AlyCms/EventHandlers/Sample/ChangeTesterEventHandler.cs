using AlyCms.Events.Sample;
using AlyCqrs.Events;
using AlyCqrs.Synhronizers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCms.EventHandlers.Sample
{
    public class ChangeTesterEventHandler : IEventHandler<ChangeTesterEvent>
    {
        private readonly ISynhronizer _synhronizer;

        public ChangeTesterEventHandler(ISynhronizer synhronizer)
        {
            _synhronizer = synhronizer;
        }

        public async Task HandleAsync(ChangeTesterEvent evnt)
        {
            await _synhronizer.ExceuteAsync(@"update [tester] set [title]=@title,[disable]=@disable where [id]=@id", new { id = evnt.AggregateKey, title = evnt.Title, disable = evnt.Disable });
        }
    }
}
