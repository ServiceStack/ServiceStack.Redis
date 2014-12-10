//Copyright (c) Service Stack LLC. All Rights Reserved.
//License: https://raw.github.com/ServiceStack/ServiceStack/master/license.txt

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ServiceStack.Caching;
using ServiceStack.Logging;

namespace ServiceStack.Redis
{
    public class RedisPoolConfig
    {
        public const int DefaultMaxPoolSize = 40;

        public RedisPoolConfig()
        {
            MaxPoolSize = DefaultMaxPoolSize;
        }

        public int MaxPoolSize { get; set; }
    }

    /// <summary>
    /// Provides thread-safe pooling of redis client connections.
    /// </summary>
    public partial class RedisManagerPool
        : IRedisClientsManager, IRedisFailover, IHandleClientDispose
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(RedisManagerPool));

        private const string PoolTimeoutError =
            "Redis Timeout expired. The timeout period elapsed prior to obtaining a connection from the pool. This may have occurred because all pooled connections were in use.";

        public int RecheckPoolAfterMs = 100;

        private List<RedisEndpoint> Hosts { get; set; }

        public List<Action<IRedisClientsManager>> OnFailover { get; private set; }

        private RedisClient[] clients = new RedisClient[0];
        protected int poolIndex;

        protected int RedisClientCounter = 0;

        public IRedisClientFactory RedisClientFactory { get; set; }

        public Action<IRedisNativeClient> ConnectionFilter { get; set; }

        public int MaxPoolSize { get; set; }

        public RedisManagerPool() : this(RedisNativeClient.DefaultHost) {}
        public RedisManagerPool(string host) : this(new[]{ host }) {}
        public RedisManagerPool(IEnumerable<string> hosts) : this(hosts, null) {}

        public RedisManagerPool(IEnumerable<string> hosts, RedisPoolConfig config)
        {
            if (hosts == null)
                throw new ArgumentNullException("hosts");

            Hosts = hosts.ToRedisEndPoints();

            if (Hosts.Count == 0)
                throw new Exception("Must provide at least ");

            this.RedisClientFactory = Redis.RedisClientFactory.Instance;

            if (config == null)
                config = new RedisPoolConfig();

            this.OnFailover = new List<Action<IRedisClientsManager>>();

            this.MaxPoolSize = config.MaxPoolSize;

            clients = new RedisClient[MaxPoolSize];
            poolIndex = 0;
        }

        public void FailoverTo(params string[] readWriteHosts)
        {
            lock (clients)
            {
                for (var i = 0; i < clients.Length; i++)
                {
                    clients[i] = null;
                }
                Hosts = readWriteHosts.ToRedisEndPoints();
            }

            if (this.OnFailover != null)
            {
                foreach (var callback in OnFailover)
                {
                    try
                    {
                        callback(this);
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Error firing OnFailover callback(): ", ex);
                    }
                }
            }
        }

        public void FailoverTo(IEnumerable<string> readWriteHosts, IEnumerable<string> readOnlyHosts)
        {
            FailoverTo(readWriteHosts.ToArray()); //only use readWriteHosts
        }

        /// <summary>
        /// Returns a Read/Write client (The default) using the hosts defined in ReadWriteHosts
        /// </summary>
        /// <returns></returns>
        public IRedisClient GetClient()
        {
            lock (clients)
            {
                AssertValidPool();

                RedisClient inActiveClient;
                while ((inActiveClient = GetInActiveClient()) == null)
                {
                    //Create new client outside of pool when max pool size exceeded
                    var nextIndex = poolIndex % Hosts.Count;
                    var nextHost = Hosts[nextIndex];
                    var newClient = InitNewClient(nextHost);
                    //Don't handle callbacks for new client outside pool
                    newClient.ClientManager = null; 
                    return newClient;
                }

                poolIndex++;
                inActiveClient.Active = true;

                InitClient(inActiveClient);

                return inActiveClient;
            }
        }

        public IRedisClient GetReadOnlyClient()
        {
            return GetClient();
        }

        /// <summary>
        /// Called within a lock
        /// </summary>
        /// <returns></returns>
        private RedisClient GetInActiveClient()
        {
            var desiredIndex = poolIndex % clients.Length;
            //this will loop through all hosts in readClients once even though there are 2 for loops
            //both loops are used to try to get the prefered host according to the round robin algorithm
            for (int x = 0; x < Hosts.Count; x++)
            {
                var nextHostIndex = (desiredIndex + x) % Hosts.Count;
                var nextHost = Hosts[nextHostIndex];
                for (var i = nextHostIndex; i < clients.Length; i += Hosts.Count)
                {
                    if (clients[i] != null && !clients[i].Active && !clients[i].HadExceptions)
                        return clients[i];
                    else if (clients[i] == null || clients[i].HadExceptions)
                    {
                        if (clients[i] != null)
                            clients[i].DisposeConnection();

                        var client = InitNewClient(nextHost);
                        clients[i] = client;

                        return client;
                    }
                }
            }
            return null;
        }

        private RedisClient InitNewClient(RedisEndpoint nextHost)
        {
            var client = RedisClientFactory.CreateRedisClient(nextHost);
            client.Id = Interlocked.Increment(ref RedisClientCounter);
            client.ClientManager = this;
            client.ConnectionFilter = ConnectionFilter;

            return client;
        }

        private void InitClient(RedisClient client)
        {
        }

        public void DisposeClient(RedisNativeClient client)
        {
            lock (clients)
            {
                for (var i = 0; i < clients.Length; i++)
                {
                    var writeClient = clients[i];
                    if (client != writeClient) continue;
                    client.Active = false;
                    return;
                }
            }
        }

        /// <summary>
        /// Disposes the write client.
        /// </summary>
        /// <param name="client">The client.</param>
        public void DisposeWriteClient(RedisNativeClient client)
        {
            lock (clients)
            {
                client.Active = false;
            }
        }

        public Dictionary<string, string> GetStats()
        {
            var clientsPoolSize = clients.Length;
            var clientsCreated = 0;
            var clientsWithExceptions = 0;
            var clientsInUse = 0;
            var clientsConnected = 0;

            foreach (var client in clients)
            {
                if (client == null)
                {
                    clientsCreated++;
                    continue;
                }

                if (client.HadExceptions)
                    clientsWithExceptions++;
                if (client.Active)
                    clientsInUse++;
                if (client.IsSocketConnected())
                    clientsConnected++;
            }

            var ret = new Dictionary<string, string>
            {
                {"clientsPoolSize", "" + clientsPoolSize},
                {"clientsCreated", "" + clientsCreated},
                {"clientsWithExceptions", "" + clientsWithExceptions},
                {"clientsInUse", "" + clientsInUse},
                {"clientsConnected", "" + clientsConnected},
            };

            return ret;
        }

        private void AssertValidPool()
        {
            if (clients.Length < 1)
                throw new InvalidOperationException("Need a minimum pool size of 1");
        }

        public int[] GetClientPoolActiveStates()
        {
            lock (clients)
            {
                var activeStates = new int[clients.Length];
                for (int i = 0; i < clients.Length; i++)
                {
                    var client = clients[i];
                    activeStates[i] = client == null
                        ? -1
                        : client.Active ? 1 : 0;
                }
                return activeStates;
            }
        }

        ~RedisManagerPool()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (Interlocked.Increment(ref disposeAttempts) > 1) return;

            if (disposing)
            {
                // get rid of managed resources
            }

            try
            {
                // get rid of unmanaged resources
                for (var i = 0; i < clients.Length; i++)
                {
                    Dispose(clients[i]);
                }
            }
            catch (Exception ex)
            {
                Log.Error("Error when trying to dispose of PooledRedisClientManager", ex);
            }
        }

        private int disposeAttempts = 0;

        protected void Dispose(RedisClient redisClient)
        {
            if (redisClient == null) return;
            try
            {
                redisClient.DisposeConnection();
            }
            catch (Exception ex)
            {
                Log.Error(string.Format(
                    "Error when trying to dispose of RedisClient to host {0}:{1}",
                    redisClient.Host, redisClient.Port), ex);
            }
        }

        public ICacheClient GetCacheClient()
        {
            return new RedisClientManagerCacheClient(this);
        }

        public ICacheClient GetReadOnlyCacheClient()
        {
            return new RedisClientManagerCacheClient(this) { ReadOnly = true };
        }
    }
}
