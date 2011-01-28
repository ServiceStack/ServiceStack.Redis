using System.Collections.Generic;


namespace ServiceStack.Redis.Support.Queue.Implementation
{
    /// <summary>
    /// distributed work item queue
    /// </summary>
    public class RedisWorkQueue<T> 
    {
        protected RedisNamespace queueNamespace = new RedisNamespace(typeof(T) + "_Shared_Work_Queue");
        protected string pendingWorkItemIdQueue = "PendingWorkItemIdQueue";
        protected string workQueue = "WorkQueue";
        protected readonly PooledRedisClientManager clientManager;

        public RedisWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port )
        {
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
        /// customize key namespace for this queue
        /// </summary>
        /// <param name="name"></param>
        public void SetQueueNamespace(string name)
        {
            queueNamespace = new RedisNamespace(name);
        }

        public void Dispose()
        {
            clientManager.Dispose();
        }
    }
}