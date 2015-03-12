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

        private readonly string sentinelName;
        private int failures = 0;
        private int sentinelIndex = -1;
        private List<string> sentinels;
        private RedisSentinelWorker worker;
        private static int MaxFailures = 5;

        public IRedisClientsManager RedisManager { get; set; }
        public Action<IRedisClientsManager> OnFailover { get; set; }
        public Action<Exception> OnWorkerError { get; set; }
        public Action<string, string> OnSentinelMessageReceived { get; set; }

        public RedisSentinel(string sentinelHost, string sentinelName)
            : this(new[] { sentinelHost }, sentinelName) { }

        public RedisSentinel(IEnumerable<string> sentinelHosts, string sentinelName)
        {
            this.sentinels = sentinelHosts != null ? sentinelHosts.ToList() : null;
            if (sentinelHosts == null || sentinels.Count == 0)
                throw new ArgumentException("sentinels must have at least one entry");

            this.sentinelName = sentinelName;
            this.RedisManagerFactory = new RedisManagerFactory();
        }

        [Obsolete("Use Start()")]
        public IRedisClientsManager Setup()
        {
            return Start();
        }

        [Obsolete("Use Start()")]
        public IRedisClientsManager Setup(int initialDb, int? poolSizeMultiplier, int? poolTimeOutSeconds)
        {
            return Start(initialDb, poolSizeMultiplier, poolTimeOutSeconds);
        }

        /// <summary>
        /// Initialize Sentinel Subscription and Configure Redis ClientsManager
        /// </summary>
        public IRedisClientsManager Start()
        {
            GetValidSentinel(null, null, null);

            if (this.RedisManager == null)
                throw new ApplicationException("Unable to resolve sentinels!");

            return this.RedisManager;
        }

        public IRedisClientsManager Start(int initialDb, int? poolSizeMultiplier, int? poolTimeOutSeconds)
        {
            GetValidSentinel(initialDb, poolSizeMultiplier, poolTimeOutSeconds);

            if (this.redisManager == null)
            {
                throw new ApplicationException("Unable to resolve sentinels!");
            }

            return this.redisManager;
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

	private RedisSentinelWorker GetValidSentinel(int? initialDb, int? poolSizeMultiplier, int? poolTimeOutSeconds)
        {
            if (this.worker != null)
                return this.worker;

            RedisException lastEx = null;

            while (this.RedisManager == null && ShouldRetry())
            {
                try
                {
                    this.worker = GetNextSentinel();

		    if (initialDb == null)
                    	this.RedisManager = worker.GetClientManager();
		    else
			this.redisManager = worker.GetClientManager(initialDb.Value, poolSizeMultiplier, poolTimeOutSeconds);

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

            var sentinelWorker = new RedisSentinelWorker(this, sentinels[sentinelIndex], this.sentinelName)
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
    public string[] RedisMasters { get; set; }
    public string[] RedisSlaves { get; set; }

    public SentinelInfo(List<string> redisMasters, List<string> redisSlaves)
    {
        RedisMasters = redisMasters != null ? redisMasters.ToArray() : new string[0];
        RedisSlaves = redisSlaves != null ? redisSlaves.ToArray() : new string[0];
    }

    public override string ToString()
    {
        return "masters: {0}, slaves: {1}".Fmt(RedisMasters, RedisSlaves);
    }
}
