using System;

namespace ServiceStack.Redis
{

    public partial class PooledRedisClientManager: IRedisClientCacheManager
    {
        /// <summary>
        /// Manage a client acquired from the PooledRedisClientManager
        /// Dispose method will release the client back to the pool.
        /// </summary>
        public class DisposablePooledClient<T> : IDisposable where T : RedisNativeClient
        {
            private T client;
            private readonly PooledRedisClientManager clientManager;

            /// <summary>
            /// wrap the acquired client
            /// </summary>
            /// <param name="clientManager"></param>
            public DisposablePooledClient(PooledRedisClientManager clientManager)
            {
                this.clientManager = clientManager;
                if (clientManager != null)
                    client = (T)clientManager.GetClient();
            }

            /// <summary>
            /// access the wrapped client
            /// </summary>
            public T Client { get { return client; } }

            /// <summary>
            /// release the wrapped client back to the pool
            /// </summary>
            public void Dispose()
            {
                if (client != null)
                    clientManager.DisposeClient(client);
                client = null;
            }
        }

        public DisposablePooledClient<T> GetDisposableClient<T>() where T : RedisNativeClient
        {
            return new DisposablePooledClient<T>(this);
        }

    }
}
