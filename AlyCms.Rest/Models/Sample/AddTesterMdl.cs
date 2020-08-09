using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace AlyCms.Rest.Models.Sample
{
    public class AddTesterMdl
    {
        [Required(ErrorMessage = "标题不能为空!")]
        [RegularExpression(@"^[a-zA-Z0-9\u4e00-\u9fa5]{2,32}$", ErrorMessage = "标题规则为2~32位的字母或数字或中文!")]
        public string Title { get; set; }

        public bool Disable { get; set; }


        //[RegularExpression(@"(\/[\d\w]+)+$", ErrorMessage = "路径不正确")]
        //public string Url { get; set; }

        //[RegularExpression(@"^[a-zA-Z0-9\-\s]{0,64}$", ErrorMessage = "图标格式不正确")]
        //public string Icon { get; set; }

        //[MaxLength(1024, ErrorMessage = "备注信息最多输入1024个字符")]
        //public string Remarks { get; set; }
    }
}
