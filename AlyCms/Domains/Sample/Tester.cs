using AlyCms.Events.Sample;
using AlyCqrs.Domains;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCms.Domains.Sample
{
    [Serializable]
    public class Tester : AggregateRoot
    {
        public string Title { get; set; }

        public bool Disable { get; set; }

        public Tester() { }

        public Tester(string title,bool disable) {
            ApplyEvent(new CreateTesterEvent(title,disable));
        }

        public void Update(Guid aggregateKey,  string title,bool disable)
        {
            ApplyEvent(new ChangeTesterEvent(Version + 1, aggregateKey, title,disable));
        }

        public void Abolish(Guid aggregateKey)
        {
            ApplyEvent(new AbolishTesterEvent(Version + 1, aggregateKey));
        }

        public void Handler(CreateTesterEvent createTesterEvent)
        {
            Id = createTesterEvent.AggregateKey;
            Version = createTesterEvent.Version;
            Title = createTesterEvent.Title;
            Disable = createTesterEvent.Disable;
        }

        public void Handler(ChangeTesterEvent changeTesterEvent)
        {
            Id = changeTesterEvent.AggregateKey;
            Version = changeTesterEvent.Version;
            Title = changeTesterEvent.Title;
            Disable = changeTesterEvent.Disable;
        }

        public void Handler(AbolishTesterEvent abolishTesterEvent)
        {
            Id = abolishTesterEvent.AggregateKey;
            Version = abolishTesterEvent.Version;
            Disable = true;
        }
    }
}
