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

namespace ServiceStack.Redis
{
    public class RedisSentinel : IRedisSentinel
    {
        public RedisManagerFactory RedisManagerFactory { get; set; }

        private readonly string sentinelName;
        private int failures = 0;
        private int sentinelIndex = -1;
        private List<string> sentinels;
        private RedisSentinelWorker worker;
        internal IRedisClientsManager redisManager;
        private static int MaxFailures = 5;

        public RedisSentinel(IEnumerable<string> sentinelHosts, string sentinelName)
        {
            this.sentinels = sentinelHosts != null ? sentinelHosts.ToList() : null;
            if (sentinelHosts == null || sentinels.Count == 0) 
                throw new ArgumentException("sentinels must have at least one entry");

            this.sentinelName = sentinelName;
            this.RedisManagerFactory = new RedisManagerFactory();
        }

        /// <summary>
        /// Initialize channel and register client manager
        /// </summary>
        /// <param name="container"></param>
        public IRedisClientsManager Setup()
        {
            GetValidSentinel();

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

        private RedisSentinelWorker GetValidSentinel()
        {
            if (this.worker != null)
                return this.worker;

            RedisException lastEx = null;

            while (this.redisManager == null && ShouldRetry())
            {
                try
                {
                    this.worker = GetNextSentinel();
                    this.redisManager = worker.GetClientManager();
                    this.worker.BeginListeningForConfigurationChanges();
                    return this.worker;
                }
                catch (RedisException ex)
                {
                    lastEx = ex;
                    if (this.worker != null)
                    {
                        this.worker.SentinelError -= Worker_SentinelError;
                        this.worker.Dispose();
                    }

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
            {
                sentinelIndex = 0;
            }

            var sentinelWorker = new RedisSentinelWorker(this, sentinels[sentinelIndex], this.sentinelName);

            sentinelWorker.SentinelError += Worker_SentinelError;
            return sentinelWorker;
        }

        /// <summary>
        /// Raised if there is an error from a sentinel worker
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void Worker_SentinelError(object sender, EventArgs e)
        {
            var worker = sender as RedisSentinelWorker;

            if (worker != null)
            {
                // dispose the worker
                worker.SentinelError -= Worker_SentinelError;
                worker.Dispose();

                // get a new worker and start looking for more changes
                this.worker = GetNextSentinel();
                this.worker.BeginListeningForConfigurationChanges();
            }
        }

        public SentinelInfo FailoverToSentinelHosts()
        {
            var worker = GetValidSentinel();
            return worker.ConfigureRedisFromSentinel();            
        }

        public void Dispose()
        {
            if (worker != null)
            {
                worker.SentinelError -= Worker_SentinelError;
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
        RedisSlaves = redisSlaves != null ? RedisSlaves.ToArray() : new string[0];
    }

    public override string ToString()
    {
        return "masters: {0}, slaves: {1}".Fmt(RedisMasters, RedisSlaves);
    }
}
