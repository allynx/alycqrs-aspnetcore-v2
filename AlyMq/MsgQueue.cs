using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace AlyMq
{
    [Serializable]
    public class MsgQueue
    {
        public Guid Key { get; set; }

        public Guid TopicKey { get; set; }

        public string Name { get; set; }

        public DateTime CreateOn { get; set; }

        public ConcurrentQueue<Msg> Queue { get; set; }
    }
}
