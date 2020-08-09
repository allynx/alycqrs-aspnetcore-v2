using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace AlyMq
{
    public class TopicComparer : IEqualityComparer<Topic>
    {
        public bool Equals(Topic x, Topic y)
        {
            return x == y || x.Key == y.Key && x.Name == y.Name && x.Tag == y.Tag && x.BrokerKey == y.BrokerKey && x.CreateOn == y.CreateOn;
        }

        public int GetHashCode(Topic obj)
        {
            return obj.Key.GetHashCode() ^ obj.BrokerKey.GetHashCode() ^ obj.Name.GetHashCode() ^ obj.Tag.GetHashCode() ^ obj.CreateOn.GetHashCode();
        }
    }
}
