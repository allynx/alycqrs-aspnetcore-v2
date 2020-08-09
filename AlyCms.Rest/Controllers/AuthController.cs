using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection.Metadata;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace AlyCms.Rest.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class AuthController : ControllerBase
    {

        [HttpPost("SignUp")]
        public async Task SignUp(object obj)
        {
            await Task.CompletedTask;
        }

        [HttpPost("SignIn")]
        public async Task SignIn(object obj)
        {
            await Task.CompletedTask;
        }

        [HttpPost("SignOut")]
        public async Task< dynamic> SignOut()
        {
           await Task.CompletedTask;
           return  new { code= HttpStatusCode.OK, status=true, msg="成功" };
        }
    }
}
