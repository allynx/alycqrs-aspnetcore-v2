using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyMq.Adapter
{
    /// <summary>
    /// Adapter service.
    /// </summary>
    public interface IAdapterService
    {
        Task Start();

        Task Stop();
    }
}
