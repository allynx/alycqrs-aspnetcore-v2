using System;
using System.Collections.Generic;
using System.Text;

namespace AlyMq
{
    /// <summary>
    /// Queue message.
    /// </summary>
    [Serializable]
    public class Msg
    {
        /// <summary>
        /// Message Key.
        /// </summary>
        public Guid Key { get; set; }

        /// <summary>
        /// Message Topic.
        /// </summary>
        public Guid TopicKey { get; set; }

        /// <summary>
        /// Message bytes content.
        /// </summary>
        public byte[] Body { get; set; }

        /// <summary>
        /// Message create date time.
        /// </summary>

        public DateTime CreateOn { get; set; }
    }
}
