using AlyCms.Domains.Sample;
using AlyCms.Events.Sample;
using AlyCqrs.Events;
using AlyCqrs.Synhronizers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCms.EventHandlers.Sample
{
    public class AbolishTesterEventHandler : IEventHandler<AbolishTesterEvent>
    {
        private readonly ISynhronizer _synhronizer;

        public AbolishTesterEventHandler(ISynhronizer synhronizer)
        {
            _synhronizer = synhronizer;
        }

        public async Task HandleAsync(AbolishTesterEvent evnt)
        {
            await _synhronizer.ExceuteAsync(@"update [tester] set [disable]=@disable where [id]=@id", new { id = evnt.AggregateKey, disable = true });
        }
    }
}
