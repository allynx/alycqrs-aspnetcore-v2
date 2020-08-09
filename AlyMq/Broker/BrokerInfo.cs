using AlyMq.Broker;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace AlyMq.Broker
{
    [Serializable]
    public class BrokerInfo
    {
        public Guid Key { get; set; }

        public string Name { get; set; }

        public string Ip { get; set; }

        public int Port { get; set; }

        public int Backlog { get; set; }

        public DateTime CreateOn { get; set; }

        public DateTime PulseOn { get; set; }
    }
}
