using System.Collections.Generic;


namespace ServiceStack.Redis.Support.Queue.Implementation
{
    /// <summary>
    /// simple distributed work item queue 
    /// 
    /// 
    /// </summary>
    public class RedisSimpleWorkQueue<T> : RedisWorkQueue<T>, ISimpleWorkQueue<T> where T : class
    {
        public RedisSimpleWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port )
            : base(maxReadPoolSize, maxWritePoolSize, host, port)
        {
        }

        /// <summary>
        /// Queue incoming messages
        /// </summary>
        /// <param name="msg"></param>
        public void Enqueue(T msg)
        {
            var key = queueNamespace.GlobalCacheKey(pendingWorkItemIdQueue);
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;
                client.RPush(key, client.Serialize(msg));
            }
        }
        
        /// <summary>
        /// Dequeue next batch of work items for processing. After this method is called,
        /// no other work items with same id will be available for
        /// dequeuing until PostDequeue is called
        /// </summary>
        /// <returns>KeyValuePair: key is work item id, and value is list of dequeued items.
        /// </returns>
        public IList<T> Dequeue(int maxBatchSize)
        {
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;
                var dequeueItems = new List<T>();
                var key = queueNamespace.GlobalCacheKey(pendingWorkItemIdQueue);
                int workItemCount = 0;
                while (client.LLen(key) > 0 && workItemCount < maxBatchSize)
                {
                    var workItem = (T) client.Deserialize(client.LPop(key));
                    if (workItem == null) continue;
                    dequeueItems.Add(workItem);
                    workItemCount++;
                }
                return dequeueItems;
            }
        }
    }
}