//
// Redis Sentinel will connect to a Redis Sentinel Instance and create an IRedisClientsManager based off of the first sentinel that returns data
//
// Upon failure of a sentinel, other sentinels will be attempted to be connected to
// Upon a s_down event, the RedisClientsManager will be failed over to the new set of slaves/masters
//

using ServiceStack;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Web;

namespace ServiceStack.Redis
{
    public class RedisSentinel : IRedisSentinel
    {
        private readonly string sentinelName;
        private int failures = 0;
        private int sentinelIndex = -1;
        private List<string> sentinels;
        private RedisSentinelWorker worker;
        private PooledRedisClientManager clientManager;
        private static int MaxFailures = 5;

        public RedisSentinel(IEnumerable<string> sentinelHosts, string sentinelName)
        {
            if (sentinelHosts == null || sentinelHosts.Count() == 0) throw new ArgumentException("sentinels must have at least one entry");

            this.sentinelName = sentinelName;
            this.sentinels = new List<string>(sentinelHosts);
        }

        /// <summary>
        /// Initialize channel and register client manager
        /// </summary>
        /// <param name="container"></param>
        public IRedisClientsManager Setup()
        {
            GetValidSentinel();

            if (this.clientManager == null)
            {
                throw new ApplicationException("Unable to resolve sentinels!");
            }

            return this.clientManager;
        }

        private void GetValidSentinel()
        {
            while (this.clientManager == null && ShouldRetry())
            {
                try
                {
                    this.worker = GetNextSentinel();
                    this.clientManager = worker.GetClientManager();
                    this.worker.BeginListeningForConfigurationChanges();
                }
                catch (RedisException)
                {
                    if (this.worker != null)
                    {
                        this.worker.SentinelError -= Worker_SentinelError;
                        this.worker.Dispose();
                    }

                    this.failures++;
                }
            }
        }

        /// <summary>
        /// Check if GetValidSentinel should try the next sentinel server
        /// </summary>
        /// <returns></returns>
        /// <remarks>This will be true if the failures is less than either RedisSentinel.MaxFailures or the # of sentinels, whatever is greater</remarks>
        private bool ShouldRetry()
        {
            return this.failures < Math.Max(RedisSentinel.MaxFailures, this.sentinels.Count);
        }

        private RedisSentinelWorker GetNextSentinel()
        {
            sentinelIndex++;

            if (sentinelIndex >= sentinels.Count)
            {
                sentinelIndex = 0;
            }

            var sentinelWorker = new RedisSentinelWorker(sentinels[sentinelIndex], this.sentinelName, this.clientManager);

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
