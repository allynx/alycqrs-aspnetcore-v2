using System;
using System.Collections.Generic;
using System.Text;

namespace AlyMq
{
    public class HostAddress
    {
        public string Ip { get; set; }
        public int Port { get; set; }
        public int Backlog { get; set; }
    }
}
