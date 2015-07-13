using ServiceStack.Logging;
using System;
using System.Collections.Generic;
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

        public Action<Exception> OnSentinelError;

        public RedisSentinelWorker(RedisSentinel redisSentinel, string host)
        {
            this.redisSentinel = redisSentinel;

            //Sentinel Servers doesn't support DB, reset to 0
            var sentinelEndpoint = host.ToRedisEndpoint(defaultPort:RedisNativeClient.DefaultPortSentinel);

            this.sentinelClient = new RedisClient(sentinelEndpoint) { Db = 0 };
            this.sentinelPubSubClient = new RedisClient(sentinelEndpoint) { Db = 0 };
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
            var c = channel.ToLower();
            if (c == "+failover-end" || c.Contains("sdown") || c.Contains("odown"))
            {
                Log.Info("Sentinel detected server down/up '{0}' with message: {1}".Fmt(channel, message));

                redisSentinel.ResetClients();
            }

            if (redisSentinel.OnSentinelMessageReceived != null)
                redisSentinel.OnSentinelMessageReceived(channel, message);
        }

        internal SentinelInfo GetSentinelInfo()
        {
            var sentinelInfo = new SentinelInfo(
                redisSentinel.MasterName,
                SanitizeMasterConfig(this.sentinelClient.SentinelMaster(redisSentinel.MasterName)),
                SanitizeSlavesConfig(this.sentinelClient.SentinelSlaves(redisSentinel.MasterName)));

            return sentinelInfo;
        }

        internal List<string> GetMasterHost(string masterName)
        {
            return sentinelClient.SentinelGetMasterAddrByName(masterName);
        }

        private List<string> SanitizeSlavesConfig(IEnumerable<Dictionary<string, string>> slaves)
        {
            string ip;
            string port;
            string flags;

            var servers = new List<string>();
            foreach (var slave in slaves)
            {
                slave.TryGetValue("flags", out flags);
                slave.TryGetValue("ip", out ip);
                slave.TryGetValue("port", out port);

                string externalIp;
                if (redisSentinel.IpAddressMap.TryGetValue(ip, out externalIp))
                    ip = externalIp;
                else if (ip == "127.0.0.1")
                    ip = this.sentinelClient.Host;

                if (ip != null && port != null && !flags.Contains("s_down") && !flags.Contains("o_down"))
                    servers.Add("{0}:{1}".Fmt(ip, port));
            }
            return servers;
        }

        private List<string> SanitizeMasterConfig(IDictionary<string, string> masterConfig)
        {
            string ip;
            string port;

            masterConfig.TryGetValue("ip", out ip);
            masterConfig.TryGetValue("port", out port);

            string ipAlias;
            if (redisSentinel.IpAddressMap.TryGetValue(ip, out ipAlias))
                ip = ipAlias;

            return ip != null && port != null
                ? new List<string>{ "{0}:{1}".Fmt(ip, port) } 
                : new List<string>();
        }

        public void BeginListeningForConfigurationChanges()
        {
            // subscribing blocks, so put it on a different thread
            Task.Factory.StartNew(SubscribeForChanges, TaskCreationOptions.LongRunning);
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
    }
}
