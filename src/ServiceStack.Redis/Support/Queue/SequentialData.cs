using System.Collections.Generic;

namespace ServiceStack.Redis.Support.Queue
{
	public class SequentialData<T> where T : class
	{
        public IList<T> WorkItems
        {
            get; set;
        }
        public IDequeueToken SequentialDequeueToken
        {
            get;set;
        }
	}
}