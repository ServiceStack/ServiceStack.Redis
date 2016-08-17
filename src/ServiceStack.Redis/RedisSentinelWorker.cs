using System.Threading;
using ServiceStack.Logging;
using System;
using System.Collections.Generic;

namespace ServiceStack.Redis
{
    internal class RedisSentinelWorker : IDisposable
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(RedisSentinelWorker));

        private readonly RedisSentinel sentinel;
        private readonly RedisClient sentinelClient;
        private RedisPubSubServer sentinePubSub;

        public Action<Exception> OnSentinelError;

        public RedisSentinelWorker(RedisSentinel sentinel, RedisEndpoint sentinelEndpoint)
        {
            this.sentinel = sentinel;
            this.sentinelClient = new RedisClient(sentinelEndpoint) {
                Db = 0, //Sentinel Servers doesn't support DB, reset to 0
                ConnectTimeout = sentinel.SentinelWorkerConnectTimeoutMs,
                ReceiveTimeout = sentinel.SentinelWorkerReceiveTimeoutMs,
                SendTimeout = sentinel.SentinelWorkerSendTimeoutMs,
            };

            if (Log.IsDebugEnabled)
                Log.Debug("Set up Redis Sentinel on {0}".Fmt(sentinelEndpoint));
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
            var isSubjectivelyDown = c.Contains("sdown");
            if (isSubjectivelyDown)
                Interlocked.Increment(ref RedisState.TotalSubjectiveServersDown);

            var isObjectivelyDown = c.Contains("odown");
            if (isObjectivelyDown)
                Interlocked.Increment(ref RedisState.TotalObjectiveServersDown);

            if (c == "+failover-end" 
                || c == "+switch-master"
                || (sentinel.ResetWhenSubjectivelyDown && isSubjectivelyDown)
                || (sentinel.ResetWhenObjectivelyDown && isObjectivelyDown))
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("Sentinel detected server down/up '{0}' with message: {1}".Fmt(channel, message));

                sentinel.ResetClients();
            }

            if (sentinel.OnSentinelMessageReceived != null)
                sentinel.OnSentinelMessageReceived(channel, message);
        }

        internal SentinelInfo GetSentinelInfo()
        {
            var masterHost = GetMasterHostInternal(sentinel.MasterName);
            if (masterHost == null)
                throw new RedisException("Redis Sentinel is reporting no master is available");

            var sentinelInfo = new SentinelInfo(
                sentinel.MasterName,
                new[] { masterHost },
                GetSlaveHosts(sentinel.MasterName));

            return sentinelInfo;
        }

        internal string GetMasterHost(string masterName)
        {
            try
            {
                return GetMasterHostInternal(masterName);
            }
            catch (Exception ex)
            {
                if (OnSentinelError != null)
                    OnSentinelError(ex);

                return null;
            }
        }

        private string GetMasterHostInternal(string masterName)
        {
            var masterInfo = sentinelClient.SentinelGetMasterAddrByName(masterName);
            return masterInfo.Count > 0
                ? SanitizeMasterConfig(masterInfo)
                : null;
        }

        private string SanitizeMasterConfig(List<string> masterInfo)
        {
            var ip = masterInfo[0];
            var port = masterInfo[1];

            string aliasIp;
            if (sentinel.IpAddressMap.TryGetValue(ip, out aliasIp))
                ip = aliasIp;

            return "{0}:{1}".Fmt(ip, port);
        }

        internal List<string> GetSentinelHosts(string masterName)
        {
            return SanitizeHostsConfig(this.sentinelClient.SentinelSentinels(sentinel.MasterName));
        }

        internal List<string> GetSlaveHosts(string masterName)
        {
            return SanitizeHostsConfig(this.sentinelClient.SentinelSlaves(sentinel.MasterName));
        }

        private List<string> SanitizeHostsConfig(IEnumerable<Dictionary<string, string>> slaves)
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

                string aliasIp;
                if (sentinel.IpAddressMap.TryGetValue(ip, out aliasIp))
                    ip = aliasIp;
                else if (ip == "127.0.0.1")
                    ip = this.sentinelClient.Host;

                if (ip != null && port != null && !flags.Contains("s_down") && !flags.Contains("o_down"))
                    servers.Add("{0}:{1}".Fmt(ip, port));
            }
            return servers;
        }

        public void BeginListeningForConfigurationChanges()
        {
            try
            {
                if (this.sentinePubSub == null)
                {
                    var sentinelManager = new BasicRedisClientManager(sentinel.SentinelHosts, sentinel.SentinelHosts) 
                    {
                        //Use BasicRedisResolver which doesn't validate non-Master Sentinel instances
                        RedisResolver = new BasicRedisResolver(sentinel.SentinelEndpoints, sentinel.SentinelEndpoints)
                    };
                    this.sentinePubSub = new RedisPubSubServer(sentinelManager)
                    {
                        HeartbeatInterval = null,
                        IsSentinelSubscription = true,
                        ChannelsMatching = new[] { RedisPubSubServer.AllChannelsWildCard },
                        OnMessage = SentinelMessageReceived
                    };
                }
                this.sentinePubSub.Start();
            }
            catch (Exception ex)
            {
                Log.Error("Error Subscribing to Redis Channel on {0}:{1}"
                    .Fmt(this.sentinelClient.Host, this.sentinelClient.Port), ex);

                if (OnSentinelError != null)
                    OnSentinelError(ex);
            }
        }

        public void ForceMasterFailover(string masterName)
        {
            this.sentinelClient.SentinelFailover(masterName);
        }

        public void Dispose()
        {
            new IDisposable[] { this.sentinelClient, sentinePubSub }.Dispose(Log);
        }
    }
}
