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
        private int lockAcquisitionTimeout = 2;
        private int lockTimeout = 2;
        protected const double  CONVENIENTLY_SIZED_FLOAT = 18014398509481984.0;

        private const int numTagsForDequeueLocked = RedisNamespace.NumTagsForLockKey + 1;
        

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
        public KeyValuePair<string, IList<T>> Dequeue(int maxBatchSize)
        {
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;

                //0. get list of all locked work ids, and unlock

                //1. get next workItemId 
                string workItemId = null;
                var dequeueItems = new List<T>();
                var smallest = client.ZRangeWithScores(pendingWorkItemIdQueue, 0, 0);
                if (smallest != null && smallest.Length > 1 && RedisNativeClient.ParseDouble(smallest[1]) != CONVENIENTLY_SIZED_FLOAT)
                {
                    workItemId = client.Deserialize(smallest[0]) as string;
                    using (var pipe = client.CreatePipeline())
                    {
                        pipe.QueueCommand(r => ((RedisNativeClient) r).ZAdd(pendingWorkItemIdQueue, CONVENIENTLY_SIZED_FLOAT, smallest[0]));
                        
                        var key = queueNamespace.GlobalCacheKey(workItemId);
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
                return new KeyValuePair<string, IList<T>>(workItemId, dequeueItems);
            }
        }

        /// <summary>
        /// Return dequeued items to front of queue. Assumes that <see cref="PostDequeue"/> 
        /// has not been called yet, so this method does not lock 
        /// </summary>
        /// <param name="workItems"></param>
        /// <param name="workItemId"></param>
        public void Requeue(string workItemId, IList<T> workItems)
        {
            if (workItems == null || workItems.Count == 0)
                return;
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;
                var key = queueNamespace.GlobalCacheKey(workItemId);

                using (var pipe = client.CreatePipeline())
                {
                    for (int i = workItems.Count - 1; i >= 0; i--)
                    {
                        int index = i;
                        pipe.QueueCommand(
                            r => ((RedisNativeClient) r).LPush(key, client.Serialize(workItems[index])));
                    }
                    pipe.QueueCommand(r => ((RedisNativeClient)r).ZIncrBy(pendingWorkItemIdQueue, -1, client.Serialize(workItemId)));
                    pipe.Flush();
                }
            }
        }
        
        /// <summary>
        /// Unlock message id, so other servers can process messages for this message id
        /// </summary>
        /// <param name="workItemId"></param>
        public void PostDequeue(string workItemId)
        {
            var key = queueNamespace.GlobalCacheKey(workItemId);
            var lockKey = queueNamespace.GlobalKey(workItemId, RedisNamespace.NumTagsForLockKey);

            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;
                using (var disposableLock = new DisposableDistributedLock(client, lockKey, lockAcquisitionTimeout, lockTimeout))
                {
                    var len = client.LLen(key);
                    if (len == 0)
                        client.ZRem(pendingWorkItemIdQueue, client.Serialize(workItemId));
                    else
                        client.ZAdd(pendingWorkItemIdQueue, len, client.Serialize(workItemId));
                }
            }
        }
    }
}