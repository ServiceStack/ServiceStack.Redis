using System;
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
    public partial class RedisSequentialWorkQueue<T> : RedisWorkQueue<T>, ISequentialWorkQueue<T> where T : class
    {
       
        private int lockAcquisitionTimeout = 2;
        private int lockTimeout = 2;
        private int dequeueLockTimeout = 300;
        protected const double  CONVENIENTLY_SIZED_FLOAT = 18014398509481984.0;

        private string dequeueIds = "DequeueIds";
        private const int numTagsForDequeueLock = RedisNamespace.NumTagsForLockKey + 1;

        public RedisSequentialWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port, int dequeueLockTimeout)
            : base(maxReadPoolSize, maxWritePoolSize, host, port)
        {
            this.dequeueLockTimeout = dequeueLockTimeout;
        }

        public RedisSequentialWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port, string queueName, int dequeueLockTimeout) 
            : base(maxReadPoolSize, maxWritePoolSize, host, port, queueName)
        {
            this.dequeueLockTimeout = dequeueLockTimeout;
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

        public SequentialData<T> Dequeue(int maxBatchSize, bool defer)
        {
            HarvestZombies();
            return DequeueImpl(maxBatchSize, defer);
        }

        private SequentialData<T> DequeueImpl(int maxBatchSize, bool defer)
        {
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;

                //1. get next workItemId 
                string workItemId = null;
                var dequeueItems = new List<T>();
                var smallest = client.ZRangeWithScores(pendingWorkItemIdQueue, 0, 0);
                IDistributedLock workItemIdLock = null;
                try
                {
                    if (smallest != null && smallest.Length > 1 && RedisNativeClient.ParseDouble(smallest[1]) != CONVENIENTLY_SIZED_FLOAT)
                    {
                        workItemId = client.Deserialize(smallest[0]) as string;

                        using (var pipe = client.CreatePipeline())
                        {
                            // track dequeue lock id
                            pipe.QueueCommand(r => ((RedisNativeClient)r).SAdd(dequeueIds, client.Serialize(workItemId)));
                            
                            // lock work item id
                            pipe.QueueCommand(r => ((RedisNativeClient)r).ZAdd(pendingWorkItemIdQueue, CONVENIENTLY_SIZED_FLOAT, smallest[0]));

                            // dequeue items
                            var key = queueNamespace.GlobalCacheKey(workItemId);
                            Action<byte[]> dequeueCallback =  x =>
                                            {
                                                if (x != null)
                                                    dequeueItems.Add((T) client.Deserialize(x));
                                            };

                            for (var i = 0; i < maxBatchSize; ++i)
                            {
                                if (defer)
                                {
                                    int index = i;
                                    pipe.QueueCommand(
                                      r => ((RedisNativeClient)r).LIndex(key, index),
                                      dequeueCallback);
                                   
                                }
                                else
                                {
                                    pipe.QueueCommand(
                                        r => ((RedisNativeClient)r).LPop(key),
                                       dequeueCallback);
                                }
                            }
                            pipe.Flush();
                        }
                        workItemIdLock = defer ?   
                            new DeferredDequeueLock(client, clientManager, this, workItemId, dequeueItems.Count) :
                                new DequeueLock(client, clientManager, this, workItemId);

                    }
                    return  new SequentialData<T>
                                 {
                                     WorkItems = dequeueItems,
                                     WorkItemId = workItemId,
                                     WorkItemIdLock = workItemIdLock
                                 };
                }
                catch (Exception)
                {
                    //release resources
                    if (workItemIdLock != null)
                        workItemIdLock.Unlock();

                    throw;
                }
            }
        }

        private void Pop(string workItemId, int itemCount)
        {
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;
                using (var pipe = client.CreatePipeline())
                {
                    var key = queueNamespace.GlobalCacheKey(workItemId);
                    for (var i = 0; i < itemCount; ++i)
                    {
                        pipe.QueueCommand(
                            r => ((RedisNativeClient)r).LPop(key));

                    }
                    pipe.Flush();

                }
            }
        }

        /// <summary>
        /// Force release of locks held by crashed servers
        /// </summary>
        public bool HarvestZombies()
        {
            bool rc = false;
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;
                var dequeueWorkItemIds = client.SMembers(dequeueIds);
                foreach (var workItemId in dequeueWorkItemIds)
                {
                    var lockId = queueNamespace.GlobalKey(client.Deserialize(workItemId), numTagsForDequeueLock);
                    var myLock = new DistributedLock(client);
                    rc |= myLock.TryForceRelease(lockId);

                }
            }
            return rc;
        }
    
        /// <summary>
        /// Unlock work item id, so other servers can process items for this id
        /// </summary>
        /// <param name="workItemId"></param>
        private void Unlock(string workItemId)
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
                        pipe.QueueCommand(r => ((RedisNativeClient)r).SRem(dequeueIds, client.Serialize(workItemId)));

                        pipe.Flush();
                    }
                   
                }
            }
        }
    }
}