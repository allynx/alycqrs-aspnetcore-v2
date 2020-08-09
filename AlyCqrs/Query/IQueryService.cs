using AlyCommon;
using SqlSugar;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Query
{
    public interface IQueryService
    {
        Task<T> GetAsync<T>(Func<SqlSugarClient, Task<T>> queryAsync);
        Task<PagedResult<T>> GetPageAsync<T>(Func<SqlSugarClient, Task<PagedResult<T>>> queryAsync);
    }
}
