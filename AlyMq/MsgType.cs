using System;
using System.Collections.Generic;
using System.Text;

namespace AlyMq
{
    public enum MsgType : int
    {
        Adapter=0,
        Broker,
        Producer,
        Consumer
    }
}
