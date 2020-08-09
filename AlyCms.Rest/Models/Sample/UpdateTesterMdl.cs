using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace AlyCms.Rest.Models.Sample
{
    public class UpdateTesterMdl
    {
        [Required(ErrorMessage = "ID不能为空!")]
        public Guid Id { get; set; }

        [Required(ErrorMessage = "标题不能为空!")]
        [RegularExpression(@"^[a-zA-Z0-9\u4e00-\u9fa5]{2,32}$", ErrorMessage = "标题规则为2~32位的字母或数字或中文!")]
        public string Title { get; set; }

        public bool Disable { get; set; }
    }
}
