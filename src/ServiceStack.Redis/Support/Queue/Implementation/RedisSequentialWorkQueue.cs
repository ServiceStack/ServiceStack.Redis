using System.Collections.Generic;
using ServiceStack.Redis.Support.Locking;


namespace ServiceStack.Redis.Support.Queue.Implementation
{
    /// <summary>
    /// distributed work item queue. Each message must have an associated
    /// work item  id. For a given id, all work items are guaranteed to be processed
    /// in the order in which they are received.
    /// 
    /// 
    /// </summary>
    public class RedisSequentialWorkQueue<T> : RedisWorkQueue<T>, ISequentialWorkQueue<T> where T : class
    {
        public class DequeueLock : DistributedLock
        {
            private RedisSequentialWorkQueue<T> workQueue;
            private string workItemId;
            public DequeueLock(IRedisClient client, RedisSequentialWorkQueue<T> workQueue, string workItemId) : base(client)
            {
                this.workQueue = workQueue;
                this.workItemId = workItemId;
            }
            public override bool Unlock()
            {
                workQueue.PostDequeue(workItemId);
                return base.Unlock();
            }
        }


        private int lockAcquisitionTimeout = 2;
        private int lockTimeout = 2;
        protected const double  CONVENIENTLY_SIZED_FLOAT = 18014398509481984.0;

        private string dequeueLockIds = "DequeueLockIds";
        private const int numTagsForDequeueLock = RedisNamespace.NumTagsForLockKey + 1;

        public RedisSequentialWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port)
            : base(maxReadPoolSize, maxWritePoolSize, host, port)
        {
        }

        public RedisSequentialWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port, string queueName) 
            : base(maxReadPoolSize, maxWritePoolSize, host, port, queueName)
        {
        }

        /// <summary>
        /// Queue incoming messages
        /// </summary>
        /// <param name="workItem"></param>
        /// <param name="workItemId"></param>
        public void Enqueue(string workItemId, T workItem)
        {
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;
                var lockKey = queueNamespace.GlobalKey(workItemId, RedisNamespace.NumTagsForLockKey);
                using (var disposableLock = new DisposableDistributedLock(client, lockKey, lockAcquisitionTimeout, lockTimeout))
                {
                    using (var pipe = client.CreatePipeline())
                    {
                        pipe.QueueCommand(r => ((RedisNativeClient)r).RPush(queueNamespace.GlobalCacheKey(workItemId), client.Serialize(workItem)));
                        pipe.QueueCommand(r => ((RedisNativeClient)r).ZIncrBy(pendingWorkItemIdQueue, -1, client.Serialize(workItemId)));
                        pipe.Flush();
                    }
                }
            }
        }

        /// <summary>
        /// Dequeue next batch of messages for processing. After this method is called,
        /// no other messages with same work item id will be returned by subsequent calles
        /// to this method until <see cref="PostDequeue"/> is called
        /// </summary>
        /// <returns>KeyValuePair: key is work item id, and value is list of dequeued items.
        /// </returns>
        public SequentialDeueueData<T> Dequeue(int maxBatchSize)
        {
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;

                //1. get next workItemId 
                string workItemId = null;
                var dequeueItems = new List<T>();
                var smallest = client.ZRangeWithScores(pendingWorkItemIdQueue, 0, 0);
                IDistributedLock dequeueLock = null;
                try
                {
                    if (smallest != null && smallest.Length > 1 && RedisNativeClient.ParseDouble(smallest[1]) != CONVENIENTLY_SIZED_FLOAT)
                    {
                        workItemId = client.Deserialize(smallest[0]) as string;

                        // acquire dequeue lock
                        var dequeueLockKey = queueNamespace.GlobalKey(workItemId, numTagsForDequeueLock);
                        dequeueLock = new DequeueLock(client, this, workItemId);
                        dequeueLock.Lock(dequeueLockKey, 2, 300);
                      
                        using (var pipe = client.CreatePipeline())
                        {
                            // track dequeue lock id
                            pipe.QueueCommand(r => ((RedisNativeClient)r).SAdd(dequeueLockIds, client.Serialize(dequeueLockKey)));
                            
                            // lock work item id
                            pipe.QueueCommand(r => ((RedisNativeClient)r).ZAdd(pendingWorkItemIdQueue, CONVENIENTLY_SIZED_FLOAT, smallest[0]));

                            var key = queueNamespace.GlobalCacheKey(workItemId);
                            // dequeue items
                            for (var i = 0; i < maxBatchSize; ++i)
                            {
                                pipe.QueueCommand(
                                    r => ((RedisNativeClient)r).LPop(key),
                                    x =>
                                    {
                                        if (x != null)
                                            dequeueItems.Add((T)client.Deserialize(x));
                                    });
                            }
                            pipe.Flush();
                        }
                    }
                    return  new SequentialDeueueData<T>
                                 {
                                     DequeueItems = dequeueItems,
                                     WorkItemId = workItemId,
                                     DequeueLock = dequeueLock
                                 };
                }
                catch (System.Exception)
                {
                    //release resources
                    PostDequeue(workItemId);
                    if (dequeueLock != null)
                        dequeueLock.Unlock();

                    throw;
                }
            }
        }

        private void HarvestZombies()
        {
            // store list of dequeued workItemIds
            // dequeue acquires dequeue lock on these ids
            // each dequeue will MGet on list of all dequeued workItemIds: then MGet on all lock keys.
            // if expired, then reacquire and PostDequeue
        }
    
        /// <summary>
        /// Unlock message id, so other servers can process messages for this message id
        /// </summary>
        /// <param name="workItemId"></param>
        private void PostDequeue(string workItemId)
        {
            if (workItemId == null)
                return;

            var key = queueNamespace.GlobalCacheKey(workItemId);
            var lockKey = queueNamespace.GlobalKey(workItemId, RedisNamespace.NumTagsForLockKey);

            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;
                using (var disposableLock = new DisposableDistributedLock(client, lockKey, lockAcquisitionTimeout, lockTimeout))
                {
                    var len = client.LLen(key);
                    using (var pipe = client.CreatePipeline())
                    {
                        // update priority queue
                        if (len == 0)
                            pipe.QueueCommand(r => ((RedisNativeClient)r).ZRem(pendingWorkItemIdQueue, client.Serialize(workItemId)) );
                        else
                            pipe.QueueCommand(r => ((RedisNativeClient)r).ZAdd(pendingWorkItemIdQueue, len, client.Serialize(workItemId)) );

                        //untrack dequeue lock
                        var dequeueLockKey = queueNamespace.GlobalKey(workItemId, numTagsForDequeueLock);
                        pipe.QueueCommand(r => ((RedisNativeClient)r).SRem(dequeueLockIds, client.Serialize(dequeueLockKey)));

                        pipe.Flush();
                    }
                   
                }
            }
        }
    }
}