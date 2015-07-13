using System;
using System.Collections.Generic;
using System.Linq;
using ServiceStack.Logging;

namespace ServiceStack.Redis
{
    public class BasicRedisResolver : IRedisResolver
    {
        static ILog log = LogManager.GetLogger(typeof(BasicRedisResolver));

        public int ReadWriteHostsCount { get; private set; }
        public int ReadOnlyHostsCount { get; private set; }

        private RedisEndpoint[] masters;
        private RedisEndpoint[] slaves;

        public RedisEndpoint[] Masters
        {
            get { return masters; }
        }
        public RedisEndpoint[] Slaves
        {
            get { return slaves; }
        }

        public BasicRedisResolver(IEnumerable<RedisEndpoint> masters, IEnumerable<RedisEndpoint> slaves)
        {
            ResetMasters(masters.ToList());
            ResetSlaves(slaves.ToList());
        }

        public virtual void ResetMasters(IEnumerable<string> hosts)
        {
            ResetMasters(hosts.ToRedisEndPoints());
        }

        public virtual void ResetMasters(List<RedisEndpoint> newMasters)
        {
            if (newMasters == null || newMasters.Count == 0)
                throw new Exception("Must provide at least 1 master");

            masters = newMasters.ToArray();
            ReadWriteHostsCount = masters.Length;

            if (log.IsDebugEnabled)
                log.Debug("New Redis Masters: " + string.Join(", ", masters.Map(x => x.GetHostString())));
        }

        public virtual void ResetSlaves(IEnumerable<string> hosts)
        {
            ResetSlaves(hosts.ToRedisEndPoints());
        }

        public RedisClient CreateRedisClient(RedisEndpoint config, bool readWrite)
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

        public virtual void ResetSlaves(List<RedisEndpoint> newSlaves)
        {
            slaves = (newSlaves ?? new List<RedisEndpoint>()).ToArray();
            ReadOnlyHostsCount = slaves.Length;

            if (log.IsDebugEnabled)
                log.Debug("New Redis Slaves: " + string.Join(", ", slaves.Map(x => x.GetHostString())));
        }
    }
}