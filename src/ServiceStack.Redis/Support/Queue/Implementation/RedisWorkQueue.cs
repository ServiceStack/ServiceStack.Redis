using System.Collections.Generic;


namespace ServiceStack.Redis.Support.Queue.Implementation
{
    /// <summary>
    /// distributed work item queue
    /// </summary>
    public class RedisWorkQueue<T> 
    {
        protected readonly RedisNamespace queueNamespace;
        protected string pendingWorkItemIdQueue = "PendingWorkItemIdQueue";
        protected string workQueue = "WorkQueue";
        protected readonly PooledRedisClientManager clientManager;

        public RedisWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port) : 
                                               this(maxReadPoolSize, maxWritePoolSize, host, port, null)
        {
           
        }

        public RedisWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port, string queueName )
        {
            var qname = queueName ?? typeof (T) + "_Shared_Work_Queue";
            queueNamespace = new RedisNamespace(qname);
            var poolConfig = new RedisClientManagerConfig
                                 {
                                     MaxReadPoolSize = maxReadPoolSize,
                                     MaxWritePoolSize = maxWritePoolSize
                                 };
            
            clientManager = new PooledRedisClientManager(new List<string>() { host + ":" + port.ToString() },new List<string>(), poolConfig)
                                {
                                    RedisClientFactory = new SerializingRedisClientFactory()
                                };
        }


        /// <summary>
        /// Return dequeued items to front of queue 
        /// </summary>
        /// <param name="workItems"></param>
        /// <param name="listId"></param>
        protected void UnDequeueImpl(IList<T> workItems, string listId)
        {
            if (workItems == null || workItems.Count == 0)
                return;
            using (var disposableClient = clientManager.GetDisposableClient<SerializingRedisClient>())
            {
                var client = disposableClient.Client;
                var key = queueNamespace.GlobalCacheKey(listId);
                using (var pipe = client.CreatePipeline())
                {
                    for (int i = workItems.Count - 1; i >= 0; i--)
                    {
                        int index = i;
                        pipe.QueueCommand(r => ((RedisNativeClient)r).LPush(key, client.Serialize(workItems[index])));
                    }
                    pipe.Flush();

                }
            }
        }


        public void Dispose()
        {
            clientManager.Dispose();
        }
    }
}