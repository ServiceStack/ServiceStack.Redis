using System.Collections.Generic;
using ServiceStack.Redis.Support.Locking;
using ServiceStack.Redis.Support.Queue.Implementation;

namespace ServiceStack.Redis.Support.Queue
{
	public class SequentialData<T> where T : class
	{
        public string WorkItemId
        {
            get; set;
        }
        public IList<T> WorkItems
        {
            get; set;
        }
        public RedisSequentialWorkQueue<T>.DequeueLock WorkItemIdLock
        {
            get;set;
        }
	}
}