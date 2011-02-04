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
    public class RedisSequentialWorkQueue<T> : RedisWorkQueue<T>, ISequentialWorkQueue<T> where T : class
    {
        public class WorkItemIdLock : DistributedLock
        {
            private bool ownsClient;
            private readonly RedisSequentialWorkQueue<T> workQueue;
            private readonly string workItemId;
            protected readonly PooledRedisClientManager clientManager;
            public WorkItemIdLock(IRedisClient client,    PooledRedisClientManager clientManager, RedisSequentialWorkQueue<T> workQueue, string workItemId) : base(client)
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
            private void ReleaseClient()
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

        private int lockAcquisitionTimeout = 2;
        private int lockTimeout = 2;
        protected const double  CONVENIENTLY_SIZED_FLOAT = 18014398509481984.0;

        private string dequeueIds = "DequeueIds";
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

        public SequentialData<T> Dequeue(int maxBatchSize)
        {
            return Peek(maxBatchSize, true);
        }
        public SequentialData<T> Peek(int maxBatchSize)
        {
            return Peek(maxBatchSize, false);
        }

        private SequentialData<T> Peek(int maxBatchSize, bool dequeue)
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

                        // acquire dequeue lock
                        var dequeueLockKey = queueNamespace.GlobalKey(workItemId, numTagsForDequeueLock);
                        workItemIdLock = new WorkItemIdLock(client, clientManager, this, workItemId);
                        workItemIdLock.Lock(dequeueLockKey, 2, 300);
                      
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
                                if (dequeue)
                                {
                                    pipe.QueueCommand(
                                        r => ((RedisNativeClient) r).LPop(key),
                                       dequeueCallback);
                                }
                                else
                                {
                                    int index = i;
                                    pipe.QueueCommand(
                                      r => ((RedisNativeClient)r).LIndex(key, index),
                                      dequeueCallback);
                                }
                            }
                            pipe.Flush();
                        }
                    }
                    return  new SequentialData<T>
                                 {
                                     WorkItems = dequeueItems,
                                     WorkItemId = workItemId,
                                     WorkItemIdLock = workItemIdLock
                                 };
                }
                catch (System.Exception)
                {
                    //release resources
                    if (workItemIdLock != null)
                        workItemIdLock.Unlock();

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
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;
                var dequeueWorkItemIds = client.SMembers(dequeueIds);
                foreach (var workItemId in dequeueWorkItemIds)
                {
                    var lockId = queueNamespace.GlobalKey(client.Deserialize(workItemId), RedisNamespace.NumTagsForLockKey);

                }
            }
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