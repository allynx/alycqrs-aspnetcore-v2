using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCqrs.Configuration
{
    public class CqrsProducerConfig
    {
        private static object padlock = new object(); 
        public static CqrsProducerConfig Instance { get; private set; }

        private CqrsProducerConfig() { }

        public static CqrsProducerConfig Create(string ip, int port, int backlog)
        {
            if (Instance == null)
            {
                lock (padlock)
                {
                    if (Instance == null)
                    {
                        Instance = new CqrsProducerConfig
                        {
                            Ip = ip,
                            Port = port,
                            Backlog = backlog
                        };
                    }
                }
            }
            return Instance;
        }
        
        public string Ip { get; private set; }

        public int Port { get; private set; }

        public int Backlog { get; private set; }

    }
}
