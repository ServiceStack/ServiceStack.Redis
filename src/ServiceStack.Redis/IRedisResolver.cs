using System;
using System.Collections.Generic;

namespace ServiceStack.Redis
{
    /// <summary>
    /// Resolver strategy for resolving hosts and creating clients
    /// </summary>
    public interface IRedisResolver
    {
        int ReadWriteHostsCount { get; }
        int ReadOnlyHostsCount { get; }

        void ResetMasters(IEnumerable<string> hosts);
        void ResetSlaves(IEnumerable<string> hosts);

        RedisClient CreateRedisClient(RedisEndpoint config, bool readWrite);

        RedisEndpoint GetReadWriteHost(int desiredIndex);
        RedisEndpoint GetReadOnlyHost(int desiredIndex);
    }

    public interface IHasRedisResolver
    {
        IRedisResolver RedisResolver { get; set; }
    }

    public class BasicRedisResolver : IRedisResolver
    {
        public int ReadWriteHostsCount { get; private set; }
        public int ReadOnlyHostsCount { get; private set; }

        private RedisEndpoint[] masters;
        private RedisEndpoint[] slaves;

        public BasicRedisResolver()
        {
            masters = new RedisEndpoint[0];
            slaves = new RedisEndpoint[0];
        }

        public virtual void ResetMasters(IEnumerable<string> hosts)
        {
            var newMasters = hosts.ToRedisEndPoints().ToArray();

            if (newMasters.Length == 0)
                throw new Exception("Must provide at least 1 master");

            masters = newMasters;
            ReadWriteHostsCount = masters.Length;
        }

        public virtual void ResetSlaves(IEnumerable<string> hosts)
        {
            slaves = hosts.ToRedisEndPoints().ToArray();
            ReadOnlyHostsCount = slaves.Length;
        }

        public virtual RedisClient CreateRedisClient(RedisEndpoint config, bool readWrite)
        {
            return RedisConfig.ClientFactory(config);
        }

        public RedisEndpoint GetReadWriteHost(int desiredIndex)
        {
            return masters[desiredIndex % masters.Length];
        }

        public RedisEndpoint GetReadOnlyHost(int desiredIndex)
        {
            return ReadOnlyHostsCount > 0
                ? slaves[desiredIndex % slaves.Length]
                : GetReadWriteHost(desiredIndex);
        }
    }
}