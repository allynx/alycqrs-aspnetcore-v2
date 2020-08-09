using AlyCms.Commands.Sample;
using AlyCms.Dto;
using AlyCms.Dto.Sample;
using AlyCms.Querys;
using AlyCms.Rest.Models.Sample;
using AlyCommon;
using AlyCqrs.Commands;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Serilog.Exceptions;

namespace AlyCms.Rest.Controllers
{
    [ApiController]
    [Route("[controller]/[action]")]
    public class SampleController : ControllerBase
    {
        private readonly ILogger<SampleController> _logger;
        private readonly ICommandBus _commandBus;
        private readonly ITesterQueryService _testerQueryService;

        public SampleController(ILogger<SampleController> logger, ICommandBus commandBus, ITesterQueryService testerQueryService, IServiceProvider provider)
        {
            _logger = logger;
            _commandBus = commandBus;
            _testerQueryService = testerQueryService;
        }

        [HttpGet]
        public async Task<PagedResult<TesterDto>> Get(int page = 1, int pageSize = 20)
        {
            return await _testerQueryService.GetPageAsync(page, pageSize);
        }

        [HttpPost]
        public async Task<dynamic> Add(AddTesterMdl mdl)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    await _commandBus.SendAsync(new CreateTesterCommand(mdl.Title, mdl.Disable));

                    return new { Status = true, Msg = "Create success!!" };
                }
                catch (Exception ex)
                {
                    return new { Status = false, Msg = ex.Message };
                }
            }
            else
            {
                return new { Status = false, Msg = "Create fail!" };
            }
        }

        [HttpPut]
        public async Task<dynamic> Change(UpdateTesterMdl mdl)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    await _commandBus.SendAsync(new ChangeTesterCommand(mdl.Id, mdl.Title, mdl.Disable));

                    return new { Status = true, Msg = "Change success!" };
                }
                catch (Exception ex)
                {
                    return new { Status = false, Msg = ex.Message };
                }
            }
            else
            {
                return new { Status = false, Msg = "Change fail!" };
            }
        }

        [HttpDelete]
        public async Task<dynamic> Abolish(KeyModel mdl)
        {

            if (ModelState.IsValid)
            {
                try
                {
                    await _commandBus.SendAsync(new AbolishTesterCommand(mdl.Id));

                    return new { Status = true, Msg = "Abolish success!!" };
                }
                catch (Exception ex)
                {
                    return new { Status = false, Msg = ex.Message };
                }
            }
            else
            {
                return new { Status = false, Msg = "Abolish fail!" };
            }
        }

        [HttpGet]
        public  Task<string> TestAdd()
        {
            DateTime dtone = DateTime.Now;

            for (int i =1; i < 101; i++)
            {
                 _commandBus.SendAsync(new CreateTesterCommand(Guid.NewGuid().ToString("N"), false));
            }


            DateTime dtwo = DateTime.Now;
            TimeSpan span = dtone.Subtract(dtwo);
            string datetime = span.Days + "天" + span.Hours + "小时" + span.Minutes + "分钟" + span.Seconds + "秒" + span.TotalDays;
            return Task.FromResult(datetime);
        }

        [HttpGet]
        public async  Task<string> TestUpdate(Guid key)
        {
            if (key == null || key == Guid.Empty) throw new ArgumentNullException("key");
            DateTime dtone = DateTime.Now;

            for (int i = 1; i < 101; i++)
            {
              await  _commandBus.SendAsync(new ChangeTesterCommand(key, i.ToString()+"_"+DateTime.Now.ToString(), false));
            }

            DateTime dtwo = DateTime.Now;
            TimeSpan span = dtone.Subtract(dtwo);
            string datetime = span.Days + "天" + span.Hours + "小时" + span.Minutes + "分钟" + span.Seconds + "秒" + span.TotalDays;
           return datetime;
        }
    }
}
