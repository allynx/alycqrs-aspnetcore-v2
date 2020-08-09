using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace AlyMq
{
    [Serializable]
    public class Topic
    {
        public Guid Key { get; set; }

        public Guid BrokerKey { get; set; }

        public string Name { get; set; }

        public string Tag { get; set; }

        public DateTime CreateOn { get; set; } = DateTime.Now;
    }
}
