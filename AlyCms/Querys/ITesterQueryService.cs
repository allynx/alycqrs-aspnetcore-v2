using AlyCms.Domains.Sample;
using AlyCms.Dto.Sample;
using AlyCommon;
using AlyCqrs.Query;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace AlyCms.Querys
{
    public interface ITesterQueryService
    {
        Task<TesterDto> GetAsync(Guid id);

        Task<PagedResult<TesterDto>> GetPageAsync(int index, int pageSize);
    }
}
