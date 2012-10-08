using System.Collections.Generic;


namespace ServiceStack.Redis.Support.Queue.Implementation
{
    /// <summary>
    /// distributed work item queue
    /// </summary>
    public class RedisWorkQueue<T> 
    {
        protected readonly RedisNamespace queueNamespace;
        protected string pendingWorkItemIdQueue;
        protected string workQueue;
        protected readonly PooledRedisClientManager clientManager;

        public RedisWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port) : 
                                               this(maxReadPoolSize, maxWritePoolSize, host, port, null)
        {
           
        }

        public RedisWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port, string queueName )
        {
            var qname = queueName ?? typeof (T) + "_Shared_Work_Queue";
            queueNamespace = new RedisNamespace(qname);
            pendingWorkItemIdQueue = queueNamespace.GlobalCacheKey("PendingWorkItemIdQueue");
            workQueue = queueNamespace.GlobalCacheKey( "WorkQueue");

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

        public void Dispose()
        {
            clientManager.Dispose();
        }
    }
}