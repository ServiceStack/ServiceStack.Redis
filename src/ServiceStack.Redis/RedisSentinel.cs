//
// Redis Sentinel will connect to a Redis Sentinel Instance and create an IRedisClientsManager based off of the first sentinel that returns data
//
// Upon failure of a sentinel, other sentinels will be attempted to be connected to
// Upon a s_down event, the RedisClientsManager will be failed over to the new set of slaves/masters
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ServiceStack;
using ServiceStack.Logging;
using ServiceStack.Text;

namespace ServiceStack.Redis
{
    public class RedisSentinel : IRedisSentinel
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(RedisSentinel));

        public static string DefaultMasterName = "mymaster";
        public static string DefaultAddress = "127.0.0.1:26379";

        private object oLock = new object();
        private bool isDisposed = false;

        private readonly string masterName;
        public string MasterName
        {
            get { return masterName; }
        }

        private int failures = 0;
        private int sentinelIndex = -1;
        public List<string> SentinelHosts { get; private set; }
        internal RedisEndpoint[] SentinelEndpoints { get; private set; }
        private RedisSentinelWorker worker;
        private static int MaxFailures = 5;

        /// <summary>
        /// Change to use a different IRedisClientsManager 
        /// </summary>
        public Func<string[], string[], IRedisClientsManager> RedisManagerFactory { get; set; }

        /// <summary>
        /// Configure the Redis Connection String to use for a Redis Client Host
        /// </summary>
        public Func<string, string> HostFilter { get; set; }

        /// <summary>
        /// The configured Redis Client Manager this Sentinel managers
        /// </summary>
        public IRedisClientsManager RedisManager { get; set; }

        /// <summary>
        /// Fired when Sentinel fails over the Redis Client Manager to a new master
        /// </summary>
        public Action<IRedisClientsManager> OnFailover { get; set; }

        /// <summary>
        /// Fired when the Redis Sentinel Worker connection fails
        /// </summary>
        public Action<Exception> OnWorkerError { get; set; }

        /// <summary>
        /// Fired when the Sentinel worker receives a message from the Sentinel Subscription
        /// </summary>
        public Action<string, string> OnSentinelMessageReceived { get; set; }

        /// <summary>
        /// Map the internal IP's returned by Sentinels to its external IP
        /// </summary>
        public Dictionary<string, string> IpAddressMap { get; set; }

        /// <summary>
        /// Whether to routinely scan for other sentinel hosts (default true)
        /// </summary>
        public bool ScanForOtherSentinels { get; set; }

        /// <summary>
        /// What interval to scan for other sentinel hosts (default 10 mins)
        /// </summary>
        public TimeSpan RefreshSentinelHostsAfter { get; set; }
        private DateTime lastSentinelsRefresh;

        /// <summary>
        /// How long to wait after failing before connecting to next redis instance (default 250ms)
        /// </summary>
        public TimeSpan WaitBetweenFailedHosts { get; set; }

        /// <summary>
        /// How long to retry connecting to hosts before throwing (default 60 secs)
        /// </summary>
        public TimeSpan MaxWaitBetweenFailedHosts { get; set; }

        /// <summary>
        /// How long to wait after consecutive failed connection attempts to master before forcing 
        /// a Sentinel to failover the current master (default 60 secs)
        /// </summary>
        public TimeSpan WaitBeforeForcingMasterFailover { get; set; }

        /// <summary>
        /// The Max Connection time for Sentinel Worker (default 100ms)
        /// </summary>
        public int SentinelWorkerConnectTimeoutMs { get; set; }

        /// <summary>
        /// The Max TCP Socket Receive time for Sentinel Worker (default 100ms)
        /// </summary>
        public int SentinelWorkerReceiveTimeoutMs { get; set; }

        /// <summary>
        /// The Max TCP Socket Send time for Sentinel Worker (default 100ms)
        /// </summary>
        public int SentinelWorkerSendTimeoutMs { get; set; }

        /// <summary>
        /// Reset client connections when Sentinel reports redis instance is subjectively down (default true)
        /// </summary>
        public bool ResetWhenSubjectivelyDown { get; set; }

        /// <summary>
        /// Reset client connections when Sentinel reports redis instance is objectively down (default true)
        /// </summary>
        public bool ResetWhenObjectivelyDown { get; set; }

        public RedisSentinel(string sentinelHost = null, string masterName = null)
            : this(new[] { sentinelHost ?? DefaultAddress }, masterName ?? DefaultMasterName) { }

        public RedisSentinel(IEnumerable<string> sentinelHosts, string masterName = null)
        {
            this.SentinelHosts = sentinelHosts != null
                ? sentinelHosts.ToList()
                : null;

            if (SentinelHosts == null || SentinelHosts.Count == 0)
                throw new ArgumentException("sentinels must have at least one entry");

            this.masterName = masterName ?? DefaultMasterName;
            IpAddressMap = new Dictionary<string, string>();
            RedisManagerFactory = (masters, slaves) => new PooledRedisClientManager(masters, slaves);
            ScanForOtherSentinels = true;
            RefreshSentinelHostsAfter = TimeSpan.FromMinutes(10);
            ResetWhenObjectivelyDown = true;
            ResetWhenSubjectivelyDown = true;
            SentinelWorkerConnectTimeoutMs = 100;
            SentinelWorkerReceiveTimeoutMs = 100;
            SentinelWorkerSendTimeoutMs = 100;
            WaitBetweenFailedHosts = TimeSpan.FromMilliseconds(250);
            MaxWaitBetweenFailedHosts = TimeSpan.FromSeconds(60);
            WaitBeforeForcingMasterFailover = TimeSpan.FromSeconds(60);
        }

        /// <summary>
        /// Initialize Sentinel Subscription and Configure Redis ClientsManager
        /// </summary>
        public IRedisClientsManager Start()
        {
            lock (oLock)
            {
                for (int i = 0; i < SentinelHosts.Count; i++)
                {
                    var parts = SentinelHosts[i].SplitOnLast(':');
                    if (parts.Length == 1)
                    {
                        SentinelHosts[i] = parts[0] + ":{0}".Fmt(RedisConfig.DefaultPortSentinel);
                    }
                }

                if (ScanForOtherSentinels)
                    RefreshActiveSentinels();

                SentinelEndpoints = SentinelHosts
                    .Map(x => x.ToRedisEndpoint(defaultPort: RedisConfig.DefaultPortSentinel))
                    .ToArray();

                var sentinelWorker = GetValidSentinelWorker();

                if (this.RedisManager == null || sentinelWorker == null)
                    throw new Exception("Unable to resolve sentinels!");
                    
                return this.RedisManager;
            }
        }

        public List<string> GetActiveSentinelHosts(IEnumerable<string> sentinelHosts)
        {
            var activeSentinelHosts = new List<string>();
            foreach (var sentinelHost in sentinelHosts.ToArray())
            {
                try
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug("Connecting to all available Sentinels to discover Active Sentinel Hosts...");

                    var endpoint = sentinelHost.ToRedisEndpoint(defaultPort: RedisConfig.DefaultPortSentinel);
                    using (var sentinelWorker = new RedisSentinelWorker(this, endpoint))
                    {
                        var activeHosts = sentinelWorker.GetSentinelHosts(MasterName);

                        if (!activeSentinelHosts.Contains(sentinelHost))
                            activeSentinelHosts.Add(sentinelHost);

                        foreach (var activeHost in activeHosts)
                        {
                            if (!activeSentinelHosts.Contains(activeHost))
                                activeSentinelHosts.Add(activeHost);
                        }
                    }

                    if (Log.IsDebugEnabled)
                        Log.Debug("All active Sentinels Found: " + string.Join(", ", activeSentinelHosts));
                }
                catch (Exception ex)
                {
                    Log.Error("Could not get active Sentinels from: {0}".Fmt(sentinelHost), ex);
                }
            }
            return activeSentinelHosts;
        }

        public void RefreshActiveSentinels()
        {
            var activeHosts = GetActiveSentinelHosts(SentinelHosts);
            if (activeHosts.Count == 0) return;

            lock (SentinelHosts)
            {
                lastSentinelsRefresh = DateTime.UtcNow;

                activeHosts.Each(x =>
                {
                    if (!SentinelHosts.Contains(x))
                        SentinelHosts.Add(x);
                });

                SentinelEndpoints = SentinelHosts
                    .Map(x => x.ToRedisEndpoint(defaultPort: RedisConfig.DefaultPortSentinel))
                    .ToArray();
            }
        }

        internal string[] ConfigureHosts(IEnumerable<string> hosts)
        {
            if (hosts == null)
                return TypeConstants.EmptyStringArray;

            return HostFilter == null
                ? hosts.ToArray()
                : hosts.Map(HostFilter).ToArray();
        }

        public SentinelInfo ResetClients()
        {
            var sentinelInfo = GetSentinelInfo();

            if (RedisManager == null)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("Configuring initial Redis Clients: {0}".Fmt(sentinelInfo));

                RedisManager = CreateRedisManager(sentinelInfo);
            }
            else
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("Failing over to Redis Clients: {0}".Fmt(sentinelInfo));

                ((IRedisFailover)RedisManager).FailoverTo(
                    ConfigureHosts(sentinelInfo.RedisMasters),
                    ConfigureHosts(sentinelInfo.RedisSlaves));
            }

            return sentinelInfo;
        }

        private IRedisClientsManager CreateRedisManager(SentinelInfo sentinelInfo)
        {
            var masters = ConfigureHosts(sentinelInfo.RedisMasters);
            var slaves = ConfigureHosts(sentinelInfo.RedisSlaves);
            var redisManager = RedisManagerFactory(masters, slaves);

            var hasRedisResolver = (IHasRedisResolver)redisManager;
            hasRedisResolver.RedisResolver = new RedisSentinelResolver(this, masters, slaves);

            var canFailover = redisManager as IRedisFailover;
            if (canFailover != null && this.OnFailover != null)
            {
                canFailover.OnFailover.Add(this.OnFailover);
            }
            return redisManager;
        }

        public IRedisClientsManager GetRedisManager()
        {
            return RedisManager ?? (RedisManager = CreateRedisManager(GetSentinelInfo()));
        }

        private RedisSentinelWorker GetValidSentinelWorker()
        {
            if (isDisposed)
                throw new ObjectDisposedException(GetType().Name);

            if (this.worker != null)
                return this.worker;

            RedisException lastEx = null;

            while (this.worker == null && ShouldRetry())
            {
                try
                {
                    this.worker = GetNextSentinel();
                    GetRedisManager();
                    this.worker.BeginListeningForConfigurationChanges();
                    this.failures = 0; //reset
                    return this.worker;
                }
                catch (RedisException ex)
                {
                    if (OnWorkerError != null)
                        OnWorkerError(ex);

                    lastEx = ex;
                    this.worker = null;
                    this.failures++;
                    Interlocked.Increment(ref RedisState.TotalFailedSentinelWorkers);
                }
            }

            this.failures = 0; //reset
            TaskUtils.Sleep(WaitBetweenFailedHosts);
            throw new RedisException("No Redis Sentinels were available", lastEx);
        }

        public RedisEndpoint GetMaster()
        {
            var sentinelWorker = GetValidSentinelWorker();
            lock (sentinelWorker)
            {
                var host = sentinelWorker.GetMasterHost(masterName);

                if (ScanForOtherSentinels && DateTime.UtcNow - lastSentinelsRefresh > RefreshSentinelHostsAfter)
                {
                    RefreshActiveSentinels();
                }

                return host != null
                    ? (HostFilter != null ? HostFilter(host) : host).ToRedisEndpoint()
                    : null;
            }
        }

        public List<RedisEndpoint> GetSlaves()
        {
            var sentinelWorker = GetValidSentinelWorker();
            lock (sentinelWorker)
            {
                var hosts = sentinelWorker.GetSlaveHosts(masterName);
                return ConfigureHosts(hosts).Map(x => x.ToRedisEndpoint());
            }
        }

        /// <summary>
        /// Check if GetValidSentinel should try the next sentinel server
        /// </summary>
        /// <returns></returns>
        /// <remarks>This will be true if the failures is less than either RedisSentinel.MaxFailures or the # of sentinels, whatever is greater</remarks>
        private bool ShouldRetry()
        {
            return this.failures < Math.Max(MaxFailures, this.SentinelEndpoints.Length);
        }

        private RedisSentinelWorker GetNextSentinel()
        {
            lock (oLock)
            {
                if (this.worker != null)
                {
                    this.worker.Dispose();
                    this.worker = null;
                }

                if (++sentinelIndex >= SentinelEndpoints.Length)
                    sentinelIndex = 0;

                var sentinelWorker = new RedisSentinelWorker(this, SentinelEndpoints[sentinelIndex])
                {
                    OnSentinelError = OnSentinelError
                };

                return sentinelWorker;
            }
        }

        private void OnSentinelError(Exception ex)
        {
            if (this.worker != null)
            {
                Log.Error("Error on existing SentinelWorker, reconnecting...");

                if (OnWorkerError != null)
                    OnWorkerError(ex);

                this.worker = GetNextSentinel();
                this.worker.BeginListeningForConfigurationChanges();
            }
        }

        public void ForceMasterFailover()
        {
            var sentinelWorker = GetValidSentinelWorker();
            lock (sentinelWorker)
            {
                sentinelWorker.ForceMasterFailover(masterName);
            }
        }

        public SentinelInfo GetSentinelInfo()
        {
            var sentinelWorker = GetValidSentinelWorker();
            lock (sentinelWorker)
            {
                return sentinelWorker.GetSentinelInfo();
            }
        }

        public void Dispose()
        {
            this.isDisposed = true;

            new IDisposable[] { RedisManager, worker }.Dispose();
        }
    }
}

public class SentinelInfo
{
    public string MasterName { get; set; }
    public string[] RedisMasters { get; set; }
    public string[] RedisSlaves { get; set; }

    public SentinelInfo(string masterName, IEnumerable<string> redisMasters, IEnumerable<string> redisSlaves)
    {
        MasterName = masterName;
        RedisMasters = redisMasters != null ? redisMasters.ToArray() : TypeConstants.EmptyStringArray;
        RedisSlaves = redisSlaves != null ? redisSlaves.ToArray() : TypeConstants.EmptyStringArray;
    }

    public override string ToString()
    {
        return "{0} masters: {1}, slaves: {2}".Fmt(
            MasterName,
            string.Join(", ", RedisMasters),
            string.Join(", ", RedisSlaves));
    }
}
