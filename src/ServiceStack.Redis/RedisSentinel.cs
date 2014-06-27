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
        private int failures = 0;
        private int sentinelIndex = -1;
        private List<string> sentinels;
        private RedisSentinelWorker worker;
        private PooledRedisClientManager clientManager;
        private static int MaxFailures = 5;

        public RedisSentinel(IEnumerable<string> sentinelHosts)
        {
            if (sentinelHosts == null || sentinelHosts.Count() == 0) throw new ArgumentException("sentinels must have at least one entry");

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
            while (this.clientManager == null && failures < RedisSentinel.MaxFailures)
            {
                try
                {
                    worker = GetNextSentinel();
                    clientManager = worker.GetClientManager();
                    worker.BeginListeningForConfigurationChanges();
                }
                catch (RedisException)
                {
                    if (worker != null)
                    {
                        worker.SentinelError -= Worker_SentinelError;
                        worker.Dispose();
                    }

                    failures++;
                }
            }
        }

        private RedisSentinelWorker GetNextSentinel()
        {
            sentinelIndex++;

            if (sentinelIndex >= sentinels.Count)
            {
                sentinelIndex = 0;
            }

            var sentinelWorker = new RedisSentinelWorker(sentinels[sentinelIndex], "mymaster", this.clientManager);

            sentinelWorker.SentinelError += Worker_SentinelError;
            return sentinelWorker;
        }

        private void Worker_SentinelError(object sender, EventArgs e)
        {
            var worker = sender as RedisSentinelWorker;

            if (worker != null)
            {
                worker.SentinelError -= Worker_SentinelError;
                worker.Dispose();

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
