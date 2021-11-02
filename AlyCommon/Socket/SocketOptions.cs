using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCommon.Socket
{
    public class SocketOptions : IOptions<SocketOptions>
    {
        public string Ip { get; set; }

        public int Port { get; set; }

        public int Backlog { get; set; }

        public SocketOptions Value => this;
    }
}
