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
        private double  CONVENIENTLY_SIZED_FLOAT = 18014398509481984.0;



        public RedisSequentialWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port)
            : base(maxReadPoolSize, maxWritePoolSize, host, port)
        {
        }

        public RedisSequentialWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port, string queueName ) 
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
        /// no other messages with same work item id will be available for
        /// dequeueing until PostDequeue is called
        /// </summary>
        /// <returns>KeyValuePair: key is work item id, and value is list of dequeued items.
        /// </returns>
        public KeyValuePair<string, IList<T>> Dequeue(int maxBatchSize)
        {
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;

                //1. get next patient id 
                string workItemId = null;
                var dequeueItems = new List<T>();
                var smallest = client.ZRangeWithScores(pendingWorkItemIdQueue, 0, 0);
                if (smallest != null && smallest.Length > 1 && RedisNativeClient.ParseDouble(smallest[1]) != CONVENIENTLY_SIZED_FLOAT)
                {
                    client.ZAdd(pendingWorkItemIdQueue, CONVENIENTLY_SIZED_FLOAT, smallest[0]);
                    var workItemCount = 0;
                    workItemId = client.Deserialize(smallest[0]) as string;
                    var key = queueNamespace.GlobalCacheKey(workItemId);
                    while (client.LLen(key) > 0 && workItemCount < maxBatchSize)
                    {
                        var workItem = (T)client.Deserialize(client.LPop(key));
                        if (workItem == null) continue;
                        dequeueItems.Add(workItem);
                        workItemCount++;
                    }
                }
                return new KeyValuePair<string, IList<T>>(workItemId, dequeueItems);
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
                    {
                        client.ZRem(pendingWorkItemIdQueue, client.Serialize(workItemId));
                    }
                    else
                    {
                        client.ZAdd(pendingWorkItemIdQueue, len, client.Serialize(workItemId));
                    }
                }
            }

        }
    }
}