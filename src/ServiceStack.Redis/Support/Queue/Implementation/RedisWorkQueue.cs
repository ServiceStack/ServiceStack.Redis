using System.Collections.Generic;


namespace ServiceStack.Redis.Support.Queue.Implementation
{
    /// <summary>
    /// distributed work item queue
    /// </summary>
    public class RedisWorkQueue<T> 
    {
        protected readonly RedisNamespace queueNamespace;
        protected const string pendingWorkItemIdQueue = "PendingWorkItemIdQueue";
        protected const string workQueue = "WorkQueue";
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

        public void Dispose()
        {
            clientManager.Dispose();
        }
    }
}