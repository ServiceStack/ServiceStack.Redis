using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ServiceStack.Logging;
using ServiceStack.Text;

namespace ServiceStack.Redis
{
    public class RedisResolver : IRedisResolver, IRedisResolverExtended
    {
        static ILog log = LogManager.GetLogger(typeof(RedisResolver));

        public Func<RedisEndpoint, RedisClient> ClientFactory { get; set; }

        public int ReadWriteHostsCount { get; private set; }
        public int ReadOnlyHostsCount { get; private set; }

        HashSet<RedisEndpoint> allHosts = new HashSet<RedisEndpoint>();

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

        public RedisResolver()
            : this(TypeConstants<RedisEndpoint>.EmptyArray, TypeConstants<RedisEndpoint>.EmptyArray) {}

        public RedisResolver(IEnumerable<string> masters, IEnumerable<string> slaves)
            : this(masters.ToRedisEndPoints(), slaves.ToRedisEndPoints()){}

        public RedisResolver(IEnumerable<RedisEndpoint> masters, IEnumerable<RedisEndpoint> slaves)
        {
            ResetMasters(masters.ToList());
            ResetSlaves(slaves.ToList());
            ClientFactory = RedisConfig.ClientFactory;
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

        public virtual RedisClient CreateRedisClient(RedisEndpoint config, bool master)
        {
            var client = ClientFactory(config);

            if (master && RedisConfig.VerifyMasterConnections)
            {
                var role = client.GetServerRole();
                if (role != RedisServerRole.Master)
                {
                    Interlocked.Increment(ref RedisState.TotalInvalidMasters);
                    log.Error("Redis Master Host '{0}' is {1}. Resetting allHosts...".Fmt(config.GetHostString(), role));
                    var newMasters = new List<RedisEndpoint>();
                    var newSlaves = new List<RedisEndpoint>();
                    RedisClient masterClient = null;
                    foreach (var hostConfig in allHosts)
                    {
                        try
                        {
                            var testClient = ClientFactory(hostConfig);
                            testClient.ConnectTimeout = RedisConfig.HostLookupTimeoutMs;
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
                        Interlocked.Increment(ref RedisState.TotalNoMastersFound);
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
        }

        public RedisClient CreateMasterClient(int desiredIndex)
        {
            return CreateRedisClient(GetReadWriteHost(desiredIndex), master: true);
        }

        public RedisClient CreateSlaveClient(int desiredIndex)
        {
            return CreateRedisClient(GetReadOnlyHost(desiredIndex), master: false);
        }
    }
}