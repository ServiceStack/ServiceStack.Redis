//
// Redis Sentinel will connect to a Redis Sentinel Instance and create an IRedisClientsManager based off of the first sentinel that returns data
//
// Upon failure of a sentinel, other sentinels will be attempted to be connected to
// Upon a s_down event, the RedisClientsManager will be failed over to the new set of slaves/masters
//

using System;
using System.Collections.Generic;
using System.Linq;
using ServiceStack;
using ServiceStack.Logging;

namespace ServiceStack.Redis
{
    public class RedisSentinel : IRedisSentinel
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(RedisSentinel));

        public RedisManagerFactory RedisManagerFactory { get; set; }

        public static string DefaultMasterName = "mymaster";
        public static string DefaultAddress = "127.0.0.1:26379";

        private readonly string masterName;
        public string MasterName
        {
            get { return masterName; }
        }

        private int failures = 0;
        private int sentinelIndex = -1;
        private List<string> sentinels;
        private RedisSentinelWorker worker;
        private static int MaxFailures = 5;

        public IRedisClientsManager RedisManager { get; set; }
        public Action<IRedisClientsManager> OnFailover { get; set; }
        public Action<Exception> OnWorkerError { get; set; }
        public Action<string, string> OnSentinelMessageReceived { get; set; }

        public Dictionary<string, string> IpAddressMap { get; set; } 

        public RedisSentinel(string sentinelHost = null, string masterName = null)
            : this(new[] { sentinelHost ?? DefaultAddress }, masterName ?? DefaultMasterName) { }

        public RedisSentinel(IEnumerable<string> sentinelHosts, string masterName = null)
        {
            this.sentinels = sentinelHosts != null ? sentinelHosts.ToList() : null;
            if (sentinelHosts == null || sentinels.Count == 0)
                throw new ArgumentException("sentinels must have at least one entry");

            this.masterName = masterName ?? DefaultMasterName;
            this.RedisManagerFactory = new RedisManagerFactory();
            IpAddressMap = new Dictionary<string, string>();
        }

        /// <summary>
        /// Initialize Sentinel Subscription and Configure Redis ClientsManager
        /// </summary>
        public IRedisClientsManager Start()
        {
            GetValidSentinel();

            if (this.RedisManager == null)
                throw new ApplicationException("Unable to resolve sentinels!");

            return this.RedisManager;
        }

        public Func<string, string> HostFilter { get; set; }

        internal string[] ConfigureHosts(IEnumerable<string> hosts)
        {
            if (hosts == null)
                return new string[0];

            return HostFilter == null
                ? hosts.ToArray()
                : hosts.Map(HostFilter).ToArray();
        }

        private RedisSentinelWorker GetValidSentinel()
        {
            if (this.worker != null)
                return this.worker;

            RedisException lastEx = null;

            while (this.RedisManager == null && ShouldRetry())
            {
                try
                {
                    this.worker = GetNextSentinel();
                    this.RedisManager = worker.GetClientManager();
                    this.worker.BeginListeningForConfigurationChanges();
                    return this.worker;
                }
                catch (RedisException ex)
                {
                    if (OnWorkerError != null)
                        OnWorkerError(ex);

                    lastEx = ex;
                    if (this.worker != null)
                        this.worker.Dispose();

                    this.failures++;
                }
            }

            throw new RedisException("RedisSentinel is not accessible", lastEx);
        }

        /// <summary>
        /// Check if GetValidSentinel should try the next sentinel server
        /// </summary>
        /// <returns></returns>
        /// <remarks>This will be true if the failures is less than either RedisSentinel.MaxFailures or the # of sentinels, whatever is greater</remarks>
        private bool ShouldRetry()
        {
            return this.failures < Math.Max(MaxFailures, this.sentinels.Count);
        }

        private RedisSentinelWorker GetNextSentinel()
        {
            sentinelIndex++;

            if (sentinelIndex >= sentinels.Count)
                sentinelIndex = 0;

            var sentinelWorker = new RedisSentinelWorker(this, sentinels[sentinelIndex], this.masterName)
            {
                OnSentinelError = OnSentinelError
            };

            return sentinelWorker;
        }

        /// <summary>
        /// Raised if there is an error from a sentinel worker
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void OnSentinelError(Exception ex)
        {
            if (this.worker != null)
            {
                Log.Error("Error on existing SentinelWorker, reconnecting...");

                if (OnWorkerError != null)
                    OnWorkerError(ex);

                // dispose the worker
                this.worker.Dispose();

                // get a new worker and start looking for more changes
                this.worker = GetNextSentinel();
                this.worker.BeginListeningForConfigurationChanges();
            }
        }

        public SentinelInfo FailoverToSentinelHosts()
        {
            return GetValidSentinel().ConfigureRedisFromSentinel();
        }

        public SentinelInfo GetSentinelInfo()
        {
            return GetValidSentinel().GetSentinelInfo();
        }

        public void Dispose()
        {
            if (worker != null)
            {
                worker.Dispose();
                worker = null;
            }
        }
    }
}

public class SentinelInfo
{
    public string MasterName { get; set; }
    public string[] RedisMasters { get; set; }
    public string[] RedisSlaves { get; set; }

    public SentinelInfo(string masterName, List<string> redisMasters, List<string> redisSlaves)
    {
        RedisMasters = redisMasters != null ? redisMasters.ToArray() : new string[0];
        RedisSlaves = redisSlaves != null ? redisSlaves.ToArray() : new string[0];
    }

    public override string ToString()
    {
        return "{0} masters: {1}, slaves: {2}".Fmt(
            MasterName,
            string.Join(", ", RedisMasters),
            string.Join(", ", RedisSlaves));
    }
}
