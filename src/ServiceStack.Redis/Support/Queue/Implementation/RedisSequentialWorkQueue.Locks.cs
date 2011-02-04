using ServiceStack.Redis.Support.Locking;
using ServiceStack.Redis.Support.Locking.Factory;


namespace ServiceStack.Redis.Support.Queue.Implementation
{
    /// <summary>
    /// distributed work item queue. Each message must have an associated
    /// work item  id. For a given id, all work items are guaranteed to be processed
    /// in the order in which they are received.
    /// 
    /// 
    /// </summary>
    public partial class RedisSequentialWorkQueue<T> : RedisWorkQueue<T>, ISequentialWorkQueue<T> where T : class
    {
        public class DequeueLockFactory : DistributedLockFactory
        {
            protected readonly RedisSequentialWorkQueue<T> workQueue;
            protected readonly string workItemId;
            protected readonly PooledRedisClientManager clientManager;
            public DequeueLockFactory(IRedisClient client, PooledRedisClientManager clientManager, RedisSequentialWorkQueue<T> workQueue, string workItemId)
                : base(client)
            {
                this.clientManager = clientManager;
                this.workQueue = workQueue;
                this.workItemId = workItemId;
            }
            public override IDistributedLock CreateLock()
            {
                return new DequeueLock(client, clientManager, workQueue, workItemId);
            }
        }

        public class DequeueLock : DistributedLock
        {
            private bool ownsClient;
            protected readonly RedisSequentialWorkQueue<T> workQueue;
            protected readonly string workItemId;
            protected readonly PooledRedisClientManager clientManager;
            public DequeueLock(IRedisClient client,    PooledRedisClientManager clientManager, RedisSequentialWorkQueue<T> workQueue, string workItemId) : base(client)
            {
                this.workQueue = workQueue;
                this.workItemId = workItemId;
                this.clientManager = clientManager;
                ownsClient = false;
            }

            public override long Lock(string key, int acquisitionTimeout, int lockTimeout)
            {
                long rc = base.Lock(key, acquisitionTimeout, lockTimeout);
                // do not hang on to the client reference. This lock may be held for a long time.
                ReleaseClient();
                return rc;
            }

            public override bool Unlock()
            {
                workQueue.Unlock(workItemId);
                bool rc =  base.Unlock();
                ReleaseClient();
                return rc;
            }
            protected void ReleaseClient()
            {
                if (ownsClient && myClient != null)
                    clientManager.DisposeClient((RedisNativeClient)myClient);
                myClient = null;
                ownsClient = false;
            }

            protected override RedisClient AcquireClient()
            {
                if (myClient == null)
                {
                    myClient = clientManager.GetClient();
                    ownsClient = true;
                }
                return (RedisClient) myClient;
            }
        }
        public class DeferredDequeueLock : DequeueLock
        {
            private readonly int numberofPeekedItems;
            public DeferredDequeueLock(IRedisClient client, PooledRedisClientManager clientManager, RedisSequentialWorkQueue<T> workQueue, string workItemId, int numberofPeekedItems)
                                                                       :base(client, clientManager, workQueue, workItemId)
            {
                this.numberofPeekedItems = numberofPeekedItems;
            }
            public override bool Unlock()
            {
                //remove items from queue
                workQueue.Pop(workItemId, numberofPeekedItems);
                
                // unlock work queue id
                workQueue.Unlock(workItemId);
                bool rc = base.Unlock();
                
                ReleaseClient();
                return rc;
            }
        }
    }
}