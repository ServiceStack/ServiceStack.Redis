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
        private DateTime harvestTime = DateTime.UtcNow;
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

 
        public SequentialData<T> Dequeue(int maxBatchSize)
        {
            //harvest zombies every 5 minutes
            var now = DateTime.UtcNow;
            var ts = now - harvestTime;
            if (ts.TotalMinutes > 5)
            {
                HarvestZombies();
                harvestTime = now;
            }

           
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;

                //1. get next workItemId 
                string workItemId = null;
                var dequeueItems = new List<T>();
                var smallest = client.ZRangeWithScores(pendingWorkItemIdQueue, 0, 0);
                DequeueLock workItemIdLock = null;
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
                                int index = i;
                                pipe.QueueCommand(
                                  r => ((RedisNativeClient)r).LIndex(key, index),
                                  dequeueCallback);

                            }
                            pipe.Flush();
                        }
                        workItemIdLock = new DequeueLock(client, clientManager, this, workItemId, dequeueItems.Count);
                        var dequeueLockKey = queueNamespace.GlobalKey(workItemId, numTagsForDequeueLock);
                        workItemIdLock.Lock(dequeueLockKey, lockAcquisitionTimeout, dequeueLockTimeout);

                    }
                    return new SequentialData<T>
                               {
                                   WorkItems = dequeueItems,
                                   SequentialDequeueToken = new DequeueLock.DequeueToken(workItemIdLock)
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
                if (dequeueWorkItemIds.Length == 0) return false;

                var keys = new string[dequeueWorkItemIds.Length];
                for (int i = 0; i < dequeueWorkItemIds.Length; ++i)
                    keys[i] = queueNamespace.GlobalKey(client.Deserialize(dequeueWorkItemIds[i]), numTagsForDequeueLock);
                var dequeueLockVals = client.MGet(keys);

                var ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
                for (int i = 0; i < dequeueLockVals.Length; ++i)
                {
                    double lockValue = (dequeueLockVals[i] != null) ? BitConverter.ToInt64(dequeueLockVals[i], 0) : 0;
                    if (lockValue < ts.TotalSeconds)
                        rc |= TryForceReleaseLock(client, (string) client.Deserialize(dequeueWorkItemIds[i]));
                }
            }
            return rc;
        }


        /// <summary>
        /// release lock held by crashed server
        /// </summary>
        /// <param name="client"></param>
        /// <param name="workItemId"></param>
        /// <returns>true if lock is released, either by this method or by another client; false otherwise</returns>
        public bool TryForceReleaseLock(SerializingRedisClient client, string workItemId)
        {
            bool rc = false;

            var dequeueLockKey = queueNamespace.GlobalKey(workItemId, numTagsForDequeueLock);
            // handle possibliity of crashed client still holding the lock
            long lockValue = 0;
            using (var pipe = client.CreatePipeline())
            {
                
                pipe.QueueCommand(r => ((RedisNativeClient) r).Watch(dequeueLockKey));
                pipe.QueueCommand(r => ((RedisNativeClient) r).Get(dequeueLockKey),
                                  x => lockValue = (x != null) ? BitConverter.ToInt64(x, 0) : 0);
                pipe.Flush();
            }

            var ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
            // no lock to release
            if (lockValue == 0)
            {
                client.UnWatch();
            }
            //lock still fresh
            else if (lockValue >= ts.TotalSeconds)
            {
                client.UnWatch();
            }
            else
            {
                // lock value is expired; try to release it, and other associated resources
                var len = client.LLen(queueNamespace.GlobalCacheKey(workItemId));
                using (var trans = client.CreateTransaction())
                {
                    //untrack dequeue lock
                    trans.QueueCommand(r => ((RedisNativeClient)r).SRem(dequeueIds, client.Serialize(workItemId)));

                    //delete dequeue lock
                    trans.QueueCommand(r => ((RedisNativeClient)r).Del(dequeueLockKey));
                    
                    // update priority queue : this will allow other clients to access this workItemId
                    if (len == 0)
                        trans.QueueCommand(r => ((RedisNativeClient)r).ZRem(pendingWorkItemIdQueue, client.Serialize(workItemId)));
                    else
                        trans.QueueCommand(r => ((RedisNativeClient)r).ZAdd(pendingWorkItemIdQueue, len, client.Serialize(workItemId)));

                    rc = trans.Commit();
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
                        //untrack dequeue lock
                        pipe.QueueCommand(r => ((RedisNativeClient)r).SRem(dequeueIds, client.Serialize(workItemId)));

                        // update priority queue
                        if (len == 0)
                            pipe.QueueCommand(r => ((RedisNativeClient)r).ZRem(pendingWorkItemIdQueue, client.Serialize(workItemId)));
                        else
                            pipe.QueueCommand(r => ((RedisNativeClient)r).ZAdd(pendingWorkItemIdQueue, len, client.Serialize(workItemId)));


                        pipe.Flush();
                    }
                   
                }
            }
        }
    }
}