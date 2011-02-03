using ServiceStack.Redis.Support.Locking;
using ServiceStack.Redis.Support.Locking.Factory;

namespace ServiceStack.Redis.Support.Queue.Implementation
{
    /// <summary>
    /// Locking strategy interface
    /// </summary>
    public class SequentialLockFactory : IDistributedLockFactory
    {
        private readonly string workItemId;
        private readonly string pendingWorkItemIdQueue;
        private readonly RedisNamespace queueNamespace;

        public SequentialLockFactory(RedisNamespace queueNamespace, string workItemId, string pendingWorkItemIdQueue)
        {
            this.queueNamespace = queueNamespace;
            this.workItemId = workItemId;
            this.pendingWorkItemIdQueue = pendingWorkItemIdQueue; 
        }
        public IDistributedLock CreateLock()
        {
            return new SequentialLock(queueNamespace, workItemId, pendingWorkItemIdQueue);
        }
    }
}