using System;
using System.Collections.Generic;
using System.Linq;
using ServiceStack.Logging;

namespace ServiceStack.Redis
{
    public class RedisSentinelResolver : IRedisResolver
    {
        static ILog log = LogManager.GetLogger(typeof(RedisResolver));

        public int ReadWriteHostsCount { get; private set; }
        public int ReadOnlyHostsCount { get; private set; }

        HashSet<RedisEndpoint> allHosts = new HashSet<RedisEndpoint>();

        private RedisSentinel sentinel;

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

        public RedisSentinelResolver(RedisSentinel sentinel)
            : this(sentinel, new RedisEndpoint[0], new RedisEndpoint[0]) { }

        public RedisSentinelResolver(RedisSentinel sentinel, IEnumerable<string> masters, IEnumerable<string> slaves)
            : this(sentinel, masters.ToRedisEndPoints(), slaves.ToRedisEndPoints()) { }

        public RedisSentinelResolver(RedisSentinel sentinel, IEnumerable<RedisEndpoint> masters, IEnumerable<RedisEndpoint> slaves)
        {
            this.sentinel = sentinel;
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
            newMasters.Each(x => allHosts.Add(x));

            if (log.IsDebugEnabled)
                log.Debug("New Redis Masters: " + string.Join(", ", masters.Map(x => x.GetHostString())));
        }

        public virtual void ResetSlaves(IEnumerable<string> hosts)
        {
            ResetSlaves(hosts.ToRedisEndPoints());
        }

        public virtual void ResetSlaves(List<RedisEndpoint> newSlaves)
        {
            slaves = (newSlaves ?? new List<RedisEndpoint>()).ToArray();
            ReadOnlyHostsCount = slaves.Length;
            newSlaves.Each(x => allHosts.Add(x));

            if (log.IsDebugEnabled)
                log.Debug("New Redis Slaves: " + string.Join(", ", slaves.Map(x => x.GetHostString())));
        }

        public virtual RedisClient CreateRedisClient(RedisEndpoint config, bool readWrite)
        {
            var client = RedisConfig.ClientFactory(config);

            if (readWrite && RedisConfig.VerifyMasterConnections)
            {
                var role = client.GetServerRole();
                if (role != RedisServerRole.Master)
                {
                    log.Error("Redis Master Host '{0}' is {1}. Resetting allHosts...".Fmt(config.GetHostString(), role));
                    var newMasters = new List<RedisEndpoint>();
                    var newSlaves = new List<RedisEndpoint>();
                    RedisClient masterClient = null;
                    foreach (var hostConfig in allHosts)
                    {
                        try
                        {
                            var testClient = new RedisClient(hostConfig) {
                                ConnectTimeout = RedisConfig.SentinelConnectTimeout
                            };
                            var testRole = testClient.GetServerRole();
                            switch (testRole)
                            {
                                case RedisServerRole.Master:
                                    newMasters.Add(hostConfig);
                                    if (masterClient == null)
                                        masterClient = testClient;
                                    break;
                                case RedisServerRole.Slave:
                                    newSlaves.Add(hostConfig);
                                    break;
                            }

                        }
                        catch { /* skip */ }
                    }

                    if (masterClient == null)
                    {
                        var errorMsg = "No master found in: " + string.Join(", ", allHosts.Map(x => x.GetHostString()));
                        log.Error(errorMsg);
                        throw new Exception(errorMsg);
                    }

                    ResetMasters(newMasters);
                    ResetSlaves(newSlaves);
                    return masterClient;
                }
            }

            return client;
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
        }    }
}