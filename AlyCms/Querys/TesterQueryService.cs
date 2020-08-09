using AlyCms.Domains.Sample;
using AlyCms.Dto.Sample;
using AlyCommon;
using AlyCqrs.Query;
using Microsoft.VisualBasic;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AlyCms.Querys
{
    public class TesterQueryService : ITesterQueryService
    {
        private readonly IQueryService _queryService;
        public TesterQueryService(IQueryService queryService)
        {
            _queryService = queryService;
        }

        public async Task<TesterDto> GetAsync(Guid id)
        {
            return await _queryService.GetAsync(async (c) => await c.Ado.SqlQuerySingleAsync<TesterDto>("select * from [tester] where [id]=@key", new { key = id }));
        }


        public async Task<PagedResult<TesterDto>> GetPageAsync(int index, int pageSize)
        {
            return await _queryService.GetPageAsync(async(c)=> {
                int total = await c.Ado.SqlQuerySingleAsync<int>("select count(id) from [tester]");
                List<TesterDto> testers = c.Ado.SqlQuery<TesterDto>("select * from Tester order by id  offset @i row fetch next @s rows only", new { i = (index - 1) * pageSize, s = pageSize });
                return new PagedResult<TesterDto>(total, (int)Math.Ceiling((decimal)total / pageSize), pageSize, index, testers);
            });
        }
    }
}
