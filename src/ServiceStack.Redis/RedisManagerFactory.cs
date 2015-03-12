using System;
using System.Collections.Generic;
using System.Linq;

namespace ServiceStack.Redis
{
    public class RedisManagerFactory
    {
        public Action<IRedisClientsManager> OnInit { get; set; }

        public Func<IEnumerable<string>, IEnumerable<string>, IRedisClientsManager> FactoryFn { get; set; }

        public RedisManagerFactory()
        {
            FactoryFn = CreatePooledRedisClientManager;
        }

        public static IRedisClientsManager CreatePooledRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts)
        {
            return readOnlyHosts.Any()
                ? new PooledRedisClientManager(readWriteHosts, readOnlyHosts)
                : new PooledRedisClientManager(readWriteHosts.ToArray());
        }

        public static IRedisClientsManager CreateBasicRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts)
        {
            return readOnlyHosts.Any()
                ? new BasicRedisClientManager(readWriteHosts, readOnlyHosts)
                : new BasicRedisClientManager(readWriteHosts.ToArray());
        }

        public static IRedisClientsManager CreateRedisManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts)
        {
            return new RedisManagerPool(readWriteHosts);
        }

        public IRedisClientsManager Create(IEnumerable<string> readWriteHosts, IEnumerable<string> readOnlyHosts)
        {
            var redisManager = FactoryFn(readWriteHosts, readOnlyHosts);
            if (OnInit != null)
                OnInit(redisManager);

            return redisManager;
        }

        public IRedisClientsManager CreatePooledRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts,
            int initialDb,
            int? poolSizeMultiplier,
            int? poolTimeOutSeconds)
        {
                return new PooledRedisClientManager(readWriteHosts, readOnlyHosts, null, initialDb, poolSizeMultiplier, poolTimeOutSeconds);
        }
    }
}