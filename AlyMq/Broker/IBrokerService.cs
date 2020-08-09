using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyMq.Broker
{
    public interface IBrokerService
    {
        Task Start();
        Task Stop();
    }
}
