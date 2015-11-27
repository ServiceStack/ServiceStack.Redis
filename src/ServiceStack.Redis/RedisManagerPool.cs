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
        : IRedisClientsManager, IRedisFailover, IHandleClientDispose, IHasRedisResolver
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(RedisManagerPool));

        private const string PoolTimeoutError =
            "Redis Timeout expired. The timeout period elapsed prior to obtaining a connection from the pool. This may have occurred because all pooled connections were in use.";

        public int RecheckPoolAfterMs = 100;

        public List<Action<IRedisClientsManager>> OnFailover { get; private set; }

        private RedisClient[] clients = new RedisClient[0];
        protected int poolIndex;

        protected int RedisClientCounter = 0;

        public Func<RedisEndpoint, RedisClient> ClientFactory { get; set; }

        public Action<IRedisNativeClient> ConnectionFilter { get; set; }

        public IRedisResolver RedisResolver { get; set; }

        public int MaxPoolSize { get; set; }

        public RedisManagerPool() : this(RedisConfig.DefaultHost) { }
        public RedisManagerPool(string host) : this(new[] { host }) { }
        public RedisManagerPool(IEnumerable<string> hosts) : this(hosts, null) { }

        public RedisManagerPool(IEnumerable<string> hosts, RedisPoolConfig config)
        {
            if (hosts == null)
                throw new ArgumentNullException("hosts");

            RedisResolver = new RedisResolver(hosts, null);

            if (config == null)
                config = new RedisPoolConfig();

            this.OnFailover = new List<Action<IRedisClientsManager>>();

            this.MaxPoolSize = config.MaxPoolSize;

            clients = new RedisClient[MaxPoolSize];
            poolIndex = 0;
        }

        public void FailoverTo(params string[] readWriteHosts)
        {
            Interlocked.Increment(ref RedisState.TotalFailovers);

            lock (clients)
            {
                for (var i = 0; i < clients.Length; i++)
                {
                    var redis = clients[i];
                    if (redis != null)
                        RedisState.DeactivateClient(redis);

                    clients[i] = null;
                }
                RedisResolver.ResetMasters(readWriteHosts);
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
            int section = 1;
            try
            {
                var inactivePoolIndex = -1;
                lock (clients)
                {
                    section = 2; 
                    AssertValidPool();

                    section = 3;
                    RedisClient inActiveClient;
                    //-1 when no available clients otherwise index of reservedSlot or existing Client
                    inactivePoolIndex = GetInActiveClient(out inActiveClient);

                    section = 4;
                    //inActiveClient != null only for Valid InActive Clients
                    if (inActiveClient != null)
                    {
                        poolIndex++;
                        inActiveClient.Active = true;

                        return inActiveClient;
                    }
                }

                //Reaches here when there's no Valid InActive Clients
                try
                {
                    section = 5;
                    //inactivePoolIndex == -1 || index of reservedSlot || index of invalid client
                    var existingClient = inactivePoolIndex >= 0 && inactivePoolIndex < clients.Length
                        ? clients[inactivePoolIndex]
                        : null;

                    section = 5;
                    if (existingClient != null && existingClient != reservedSlot && existingClient.HadExceptions)
                    {
                        section = 6;
                        RedisState.DeactivateClient(existingClient);
                    }

                    section = 7;
                    var newClient = RedisResolver.CreateMasterClient(Math.Max(inactivePoolIndex, 0));
                    section = 8;
                    newClient = InitNewClient(newClient);

                    //Put all blocking I/O or potential Exceptions before lock
                    lock (clients)
                    {
                        //Create new client outside of pool when max pool size exceeded
                        //Reverting free-slot not needed when -1 since slwo wasn't reserved or 
                        //when existingClient changed (failover) since no longer reserver
                        section = 9;
                        var stillReserved = inactivePoolIndex >= 0 && inactivePoolIndex < clients.Length &&
                                            clients[inactivePoolIndex] == existingClient;
                        if (inactivePoolIndex == -1 || !stillReserved)
                        {
                            section = 10;
                            if (Log.IsDebugEnabled)
                                Log.Debug("clients[inactivePoolIndex] != existingClient: {0}".Fmt(!stillReserved ? "!stillReserved" : "-1"));

                            Interlocked.Increment(ref RedisState.TotalClientsCreatedOutsidePool);

                            //Don't handle callbacks for new client outside pool
                            newClient.ClientManager = null;
                            return newClient;
                        }

                        section = 11;
                        poolIndex++;
                        clients[inactivePoolIndex] = newClient;
                        return newClient;
                    }
                }
                catch
                {
                    //Revert free-slot for any I/O exceptions that can throw (before lock)
                    lock (clients)
                    {
                        section = 12;
                        if (inactivePoolIndex >= 0 && inactivePoolIndex < clients.Length)
                        {
                            clients[inactivePoolIndex] = null;
                        }
                    }
                    throw;
                }
            }
            catch (IndexOutOfRangeException ex)
            {
                throw new Exception("Error after section: " + section, ex);
            }
            finally
            {
                try
                {
                    RedisState.DisposeExpiredClients();
                }
                catch (IndexOutOfRangeException ex)
                {
                    throw new Exception("Error at RedisState.DisposeExpiredClients()", ex);
                }
            }
        }

        public IRedisClient GetReadOnlyClient()
        {
            return GetClient();
        }

        class ReservedClient : RedisClient
        {
            public ReservedClient()
            {
                this.DeactivatedAt = DateTime.UtcNow;
            }

            public override void Dispose() { }
        }

        static readonly ReservedClient reservedSlot = new ReservedClient();


        /// <summary>
        /// Called within a lock
        /// </summary>
        /// <returns></returns>
        private int GetInActiveClient(out RedisClient inactiveClient)
        {
            //this will loop through all hosts in readClients once even though there are 2 for loops
            //both loops are used to try to get the prefered host according to the round robin algorithm
            var readWriteTotal = RedisResolver.ReadWriteHostsCount;
            var desiredIndex = poolIndex % clients.Length;
            for (int x = 0; x < readWriteTotal; x++)
            {
                var nextHostIndex = (desiredIndex + x) % readWriteTotal;
                for (var i = nextHostIndex; i < clients.Length; i += readWriteTotal)
                {
                    if (clients[i] != null && !clients[i].Active && !clients[i].HadExceptions)
                    {
                        inactiveClient = clients[i];
                        return i;
                    }

                    if (clients[i] == null)
                    {
                        clients[i] = reservedSlot;
                        inactiveClient = null;
                        return i;
                    }

                    if (clients[i] != reservedSlot && clients[i].HadExceptions)
                    {
                        inactiveClient = null;
                        return i;
                    }
                }
            }
            inactiveClient = null;
            return -1;
        }

        private RedisClient InitNewClient(RedisClient client)
        {
            client.Id = Interlocked.Increment(ref RedisClientCounter);
            client.Active = true;
            client.ClientManager = this;
            client.ConnectionFilter = ConnectionFilter;

            return client;
        }

        public void DisposeClient(RedisNativeClient client)
        {
            lock (clients)
            {
                for (var i = 0; i < clients.Length; i++)
                {
                    var writeClient = clients[i];
                    if (client != writeClient) continue;
                    if (client.IsDisposed)
                    {
                        clients[i] = null;
                    }
                    else
                    {
                        client.Active = false;
                    }

                    Monitor.PulseAll(clients);
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
                {"RedisResolver.ReadOnlyHostsCount", "" + RedisResolver.ReadOnlyHostsCount},
                {"RedisResolver.ReadWriteHostsCount", "" + RedisResolver.ReadWriteHostsCount},
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

            RedisState.DisposeAllDeactivatedClients();
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
