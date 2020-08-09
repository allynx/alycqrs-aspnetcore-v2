using AlyCommon;
using Microsoft.Extensions.Logging;
using SqlSugar;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks;

namespace AlyCqrs.Query
{
    public  class DefaultQueryService: IQueryService
    {
        private readonly ILogger<DefaultQueryService> _logger;
        private readonly SqlSugarClient _sqlSugarClient;
        public DefaultQueryService(ILogger<DefaultQueryService> logger)
        {
            _logger = logger;

            _sqlSugarClient = new SqlSugarClient(new ConnectionConfig()
            {
                ConnectionString = "Data Source=.;Initial Catalog=AllynCms;Integrated Security=True;Persist Security Info=False;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=False",
                DbType = DbType.SqlServer,//设置数据库类型
                IsAutoCloseConnection = true,//自动释放数据务，如果存在事务，在事务结束后释放
                InitKeyType = InitKeyType.Attribute //从实体特性中读取主键自增列信息
            });

            _sqlSugarClient.Aop.OnLogExecuting = (sql, pars) =>
            {
                _logger.LogDebug("{0}\r\n{1}\r\n{2}", "Cqrs synhronizer sql executing logger", sql, _sqlSugarClient.Utilities.SerializeObject(pars.ToDictionary(p => p.ParameterName, p => p.Value)));
            };

            _sqlSugarClient.Aop.OnError = (ex) => {
                _logger.LogError(ex, "{0}\r\n{1}", "Cqrs synhronizer sql executed error logger", ex.StackTrace);
            };
        }

        public async Task<T> GetAsync<T>(Func<SqlSugarClient, Task<T>> queryAsync) => await queryAsync(_sqlSugarClient);

        public async Task<PagedResult<T>> GetPageAsync<T>(Func<SqlSugarClient, Task<PagedResult<T>>> queryAsync) => await queryAsync(_sqlSugarClient);
    }
}
