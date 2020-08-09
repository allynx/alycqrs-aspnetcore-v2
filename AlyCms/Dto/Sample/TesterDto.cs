using System;
using System.Collections.Generic;
using System.Text;

namespace AlyCms.Dto.Sample
{
    [Serializable]
    public class TesterDto
    {
        public Guid Id { get; set; }
        public string Title { get; set; }
        public bool Disable { get; set; }
    }
}
