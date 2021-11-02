using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCqrs.Configuration
{
    public sealed class CqrsTopicConfig
    {
        private static object padlock = new object();
        public static CqrsTopicConfig Instance { get; private set; }

        private CqrsTopicConfig() { }

        public static CqrsTopicConfig Create(Guid eventTopicKey, Guid commandTopicKey, Guid synchronizeTopicKey)
        {
            if (Instance == null)
            {
                lock (padlock)
                {
                    if (Instance == null)
                    {
                        Instance = new CqrsTopicConfig
                        {
                            EventTopicKey = eventTopicKey,
                            CommandTopicKey = commandTopicKey,
                            SynchronizeTopicKey = synchronizeTopicKey
                        };
                    }
                }
            }
            return Instance;
        }


        public Guid EventTopicKey { get; private set; }

        public Guid CommandTopicKey { get; private set; }

        public Guid SynchronizeTopicKey { get;private set; }
    }
}
