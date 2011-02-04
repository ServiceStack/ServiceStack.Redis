using System.Collections.Generic;
using ServiceStack.Redis.Support.Locking;

namespace ServiceStack.Redis.Support.Queue
{
	public class SequentialDeueueData<T> where T : class
	{
        public string WorkItemId
        {
            get; set;
        }
        public IList<T> DequeueItems
        {
            get; set;
        }
        public IDistributedLock DequeueLock
        {
            get;set;
        }
	}
}