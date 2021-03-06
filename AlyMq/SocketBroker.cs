﻿using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;

namespace AlyMq
{
    public class SocketBroker
    {
        public Guid Key { get; set; }

        public string Name { get; set; }

        public Socket Socket { get; set; }

        public List<string> Topics { get; set; }
    }
}
