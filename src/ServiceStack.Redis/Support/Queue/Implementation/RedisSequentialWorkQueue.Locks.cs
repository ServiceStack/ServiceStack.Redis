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
    public partial class RedisSequentialWorkQueue<T> 
    {

        public class DequeueLock : DistributedLock
        {
            private bool ownsClient;
            protected readonly RedisSequentialWorkQueue<T> workQueue;
            protected readonly string workItemId;
            protected readonly PooledRedisClientManager clientManager;
            protected readonly int numberOfDequeuedItems;
            protected int numberOfProcessedItems;
            public DequeueLock(IRedisClient client,    PooledRedisClientManager clientManager, RedisSequentialWorkQueue<T> workQueue, string workItemId, int numberOfDequeuedItems) : base(client)
            {
                this.workQueue = workQueue;
                this.workItemId = workItemId;
                this.clientManager = clientManager;
                ownsClient = false;
                this.numberOfDequeuedItems = numberOfDequeuedItems;
            }

            public override long Lock(string key, int acquisitionTimeout, int lockTimeout)
            {
                long rc = base.Lock(key, acquisitionTimeout, lockTimeout);
                // do not hang on to the client reference. This lock may be held for a long time.
                ReleaseClient();
                return rc;
            }

            public void DoneProcessedWorkItem()
            {
                numberOfProcessedItems++;
                if (numberOfProcessedItems == numberOfDequeuedItems)
                    Unlock();
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
            public DeferredDequeueLock(IRedisClient client, PooledRedisClientManager clientManager, RedisSequentialWorkQueue<T> workQueue, string workItemId, int numberOfDequeuedItems) 
                : base(client, clientManager, workQueue, workItemId, numberOfDequeuedItems)
            {
            }

            public override bool Unlock()
            {
                return Unlock(numberOfDequeuedItems);
            }
            /// <summary>
            /// 
            /// </summary>
            /// <param name="numProcessed"></param>
            /// <returns></returns>
            public bool Unlock(int numProcessed)
            {
                if (numProcessed < 0)
                    numProcessed = 0;
                if (numProcessed > numberOfDequeuedItems)
                    numProcessed = numberOfDequeuedItems;

                //remove items from queue
                workQueue.Pop(workItemId, numProcessed);
                
                // unlock work queue id
                workQueue.Unlock(workItemId);
                bool rc = base.Unlock();
                
                ReleaseClient();
                return rc;
            }
        }
    }
}