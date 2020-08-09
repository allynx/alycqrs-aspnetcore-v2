using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyMq.Consumer
{
    public interface IConsumerService
    {
        Task Start();
        Task Stop();
    }
}
