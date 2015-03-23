using ServiceStack.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceStack.Redis
{
    internal class RedisSentinelWorker : IDisposable
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(RedisSentinelWorker));

        private readonly RedisSentinel redisSentinel;
        private readonly RedisClient sentinelClient;
        private readonly RedisClient sentinelPubSubClient;
        private readonly IRedisSubscription sentinelSubscription;
        private readonly string sentinelName;
        private string host;
        private IRedisClientsManager redisManager;

        public Action<Exception> OnSentinelError;

        public RedisSentinelWorker(RedisSentinel redisSentinel, string host, string sentinelName)
        {

            this.redisSentinel = redisSentinel;
            this.redisManager = redisSentinel.RedisManager;
            this.sentinelName = sentinelName;

            //Sentinel Servers doesn't support DB, reset to 0
            this.sentinelClient = new RedisClient(host) { Db = 0 };
            this.sentinelPubSubClient = new RedisClient(host) { Db = 0 };
            this.sentinelSubscription = this.sentinelPubSubClient.CreateSubscription();
            this.sentinelSubscription.OnMessage = SentinelMessageReceived;

            if (Log.IsDebugEnabled)
                Log.Debug("Set up Redis Sentinel on {0}".Fmt(host));
        }

        private void SubscribeForChanges(object arg)
        {
            try
            {
                // subscribe to all messages
                this.sentinelSubscription.SubscribeToChannelsMatching("*");
            }
            catch (Exception ex)
            {
                Log.Error("Error Subscribing to Redis Channel on {0}:{1}"
                    .Fmt(this.sentinelClient.Host, this.sentinelClient.Port), ex);

                if (OnSentinelError != null)
                    OnSentinelError(ex);
            }
        }

        /// <summary>
        /// Event that is fired when the sentinel subscription raises an event
        /// </summary>
        /// <param name="channel"></param>
        /// <param name="message"></param>
        private void SentinelMessageReceived(string channel, string message)
        {
            if (Log.IsDebugEnabled)
                Log.Debug("Received '{0}' on channel '{1}' from Sentinel".Fmt(channel, message));

            // {+|-}sdown is the event for server coming up or down
            if ((channel == "+failover-end") || channel.ToLower().Contains("sdown"))
            {
                Log.Info("Sentinel detected server down/up with message:{0}".Fmt(message));

                ConfigureRedisFromSentinel();
            }

            if (redisSentinel.OnSentinelMessageReceived != null)
                redisSentinel.OnSentinelMessageReceived(channel, message);
        }

        /// <summary>
        /// Does a sentinel check for masters and slaves and either sets up or fails over to the new config
        /// </summary>
        internal SentinelInfo ConfigureRedisFromSentinel()
        {
            var sentinelInfo = GetSentinelInfo();

            if (redisManager == null)
            {
                Log.Info("Configuring initial Redis Clients: {0}".Fmt(sentinelInfo));

                redisManager = redisSentinel.RedisManagerFactory.Create(
                    redisSentinel.ConfigureHosts(sentinelInfo.RedisMasters),
                    redisSentinel.ConfigureHosts(sentinelInfo.RedisSlaves));

                var canFailover = redisManager as IRedisFailover;
                if (canFailover != null && this.redisSentinel.OnFailover != null)
                {
                    canFailover.OnFailover.Add(this.redisSentinel.OnFailover);
                }
            }
            else
            {
                Log.Info("Failing over to Redis Clients: {0}".Fmt(sentinelInfo));

                ((IRedisFailover)redisManager).FailoverTo(
                    redisSentinel.ConfigureHosts(sentinelInfo.RedisMasters),
                    redisSentinel.ConfigureHosts(sentinelInfo.RedisSlaves));
            }

            return sentinelInfo;
        }

        internal SentinelInfo GetSentinelInfo()
        {
            var sentinelInfo = new SentinelInfo(
                ConvertMasterArrayToList(this.sentinelClient.Sentinel("master", this.sentinelName)),
                ConvertSlaveArrayToList(this.sentinelClient.Sentinel("slaves", this.sentinelName)));
            return sentinelInfo;
        }

        private Dictionary<string, string> ParseDataArray(object[] items)
        {
            var data = new Dictionary<string, string>();
            bool isKey = false;
            string key = null;
            string value = null;

            foreach (var item in items)
            {
                if (item is byte[])
                {
                    isKey = !isKey;

                    if (isKey)
                    {
                        key = Encoding.UTF8.GetString((byte[])item);
                    }
                    else
                    {
                        value = Encoding.UTF8.GetString((byte[])item);

                        if (!data.ContainsKey(key))
                        {
                            data.Add(key, value);
                        }
                    }
                }
            }

            return data;
        }

        /// <summary>
        /// Takes output from sentinel slaves command and converts into a list of servers
        /// </summary>
        /// <param name="items"></param>
        /// <returns></returns>
        private List<string> ConvertSlaveArrayToList(object[] slaves)
        {
            var servers = new List<string>();
            string ip = null;
            string port = null;
            string flags = null;

            foreach (var slave in slaves.OfType<object[]>())
            {
                var data = ParseDataArray(slave);

                data.TryGetValue("flags", out flags);
                data.TryGetValue("ip", out ip);
                data.TryGetValue("port", out port);

                if (ip == "127.0.0.1")
                {
                    ip = this.sentinelClient.Host;
                }

                if (ip != null && port != null && !flags.Contains("s_down") && !flags.Contains("o_down"))
                {
                    servers.Add("{0}:{1}".Fmt(ip, port));
                }
            }

            return servers;
        }

        /// <summary>
        /// Takes output from sentinel master command and converts into a list of servers
        /// </summary>
        /// <param name="items"></param>
        /// <returns></returns>
        private List<string> ConvertMasterArrayToList(object[] items)
        {
            var servers = new List<string>();
            string ip = null;
            string port = null;

            var data = ParseDataArray(items);

            data.TryGetValue("ip", out ip);
            data.TryGetValue("port", out port);

            if (ip != null && port != null)
            {
                servers.Add("{0}:{1}".Fmt(ip, port));
            }

            return servers;
        }

        public IRedisClientsManager GetClientManager()
        {
            ConfigureRedisFromSentinel();

            return this.redisManager;
        }

        public void Dispose()
        {
            this.sentinelClient.Dispose();
            this.sentinelPubSubClient.Dispose();

            try
            {
                this.sentinelSubscription.Dispose();
            }
            catch (RedisException)
            {
                // if this is getting disposed after the sentinel shuts down, this will fail
            }
        }

        public void BeginListeningForConfigurationChanges()
        {
            // subscribing blocks, so put it on a different thread
            Task.Factory.StartNew(SubscribeForChanges, TaskCreationOptions.LongRunning);
        }
    }
}
