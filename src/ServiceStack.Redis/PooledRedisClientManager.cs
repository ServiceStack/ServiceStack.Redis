//
// https://github.com/mythz/ServiceStack.Redis
// ServiceStack.Redis: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Demis Bellot (demis.bellot@gmail.com)
//
// Copyright 2010 Liquidbit Ltd.
//
// Licensed under the same terms of Redis and ServiceStack: new BSD license.
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ServiceStack.CacheAccess;
using ServiceStack.Common.Web;
using ServiceStack.Logging;

namespace ServiceStack.Redis
{
    /// <summary>
    /// Provides thread-safe pooling of redis client connections.
    /// Allows load-balancing of master-write and read-slave hosts, ideal for
    /// 1 master and multiple replicated read slaves.
    /// </summary>
    public partial class PooledRedisClientManager
        : IRedisClientsManager, IRedisFailover
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(PooledRedisClientManager));

        private const string PoolTimeoutError =
                   "Redis Timeout expired. The timeout period elapsed prior to obtaining a connection from the pool. This may have occurred because all pooled connections were in use.";

        protected readonly int PoolSizeMultiplier = 10;
        public int RecheckPoolAfterMs = 100;
        public int? PoolTimeout { get; set; }
        public int? ConnectTimeout { get; set; }
        public int? SocketSendTimeout { get; set; }
        public int? SocketReceiveTimeout { get; set; }

        /// <summary>
        /// Gets or sets object key prefix.
        /// </summary>
        public string NamespacePrefix { get; set; }

        private List<RedisEndPoint> ReadWriteHosts { get; set; }
        private List<RedisEndPoint> ReadOnlyHosts { get; set; }
        public List<Action<IRedisClientsManager>> OnFailover { get; private set; }

        private RedisClient[] writeClients = new RedisClient[0];
        protected int WritePoolIndex;

        private RedisClient[] readClients = new RedisClient[0];
        protected int ReadPoolIndex;

        protected int RedisClientCounter = 0;

        protected RedisClientManagerConfig Config { get; set; }

        public IRedisClientFactory RedisClientFactory { get; set; }

        public long Db { get; private set; }

        public Action<IRedisNativeClient> ConnectionFilter { get; set; }

        public PooledRedisClientManager() : this(RedisNativeClient.DefaultHost) { }

        public PooledRedisClientManager(int poolSize, int poolTimeOutSeconds, params string[] readWriteHosts)
            : this(readWriteHosts, readWriteHosts, null, RedisNativeClient.DefaultDb, poolSize, poolTimeOutSeconds)
        {
        }

        public PooledRedisClientManager(long initialDb, params string[] readWriteHosts)
            : this(readWriteHosts, readWriteHosts, initialDb) { }

        public PooledRedisClientManager(params string[] readWriteHosts)
            : this(readWriteHosts, readWriteHosts)
        {
        }

        public PooledRedisClientManager(IEnumerable<string> readWriteHosts, IEnumerable<string> readOnlyHosts)
            : this(readWriteHosts, readOnlyHosts, null)
        {
        }

        /// <summary>
        /// Hosts can be an IP Address or Hostname in the format: host[:port]
        /// e.g. 127.0.0.1:6379
        /// default is: localhost:6379
        /// </summary>
        /// <param name="readWriteHosts">The write hosts.</param>
        /// <param name="readOnlyHosts">The read hosts.</param>
        /// <param name="config">The config.</param>
        public PooledRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts,
            RedisClientManagerConfig config)
            : this(readWriteHosts, readOnlyHosts, config, RedisNativeClient.DefaultDb, null, null)
        {
        }

        public PooledRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts,
            long initalDb)
            : this(readWriteHosts, readOnlyHosts, null, initalDb, null, null)
        {
        }

        public PooledRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts,
            RedisClientManagerConfig config,
            long initalDb,
            int? poolSizeMultiplier,
            int? poolTimeOutSeconds)
        {
            this.Db = config != null
                ? config.DefaultDb.GetValueOrDefault(initalDb)
                : initalDb;

            ReadWriteHosts = readWriteHosts.ToRedisEndPoints();
            ReadOnlyHosts = readOnlyHosts.ToRedisEndPoints();

            this.RedisClientFactory = Redis.RedisClientFactory.Instance;

            this.PoolSizeMultiplier = poolSizeMultiplier ?? 10;

            this.Config = config ?? new RedisClientManagerConfig
            {
                MaxWritePoolSize = ReadWriteHosts.Count * PoolSizeMultiplier,
                MaxReadPoolSize = ReadOnlyHosts.Count * PoolSizeMultiplier,
            };

            this.OnFailover = new List<Action<IRedisClientsManager>>();

            // if timeout provided, convert into milliseconds
            this.PoolTimeout = poolTimeOutSeconds != null
                ? poolTimeOutSeconds * 1000
                : 2000; //Default Timeout


            if (this.Config.AutoStart)
            {
                this.OnStart();
            }
        }

        public void FailoverTo(params string[] readWriteHosts)
        {
            FailoverTo(readWriteHosts, readWriteHosts);
        }

        public void FailoverTo(IEnumerable<string> readWriteHosts, IEnumerable<string> readOnlyHosts)
        {
            lock (readClients)
            {
                for (var i = 0; i < readClients.Length; i++)
                {
                    readClients[i] = null;
                }
                ReadOnlyHosts = readOnlyHosts.ToRedisEndPoints();
            }

            lock (writeClients)
            {
                for (var i = 0; i < writeClients.Length; i++)
                {
                    writeClients[i] = null;
                }
                ReadWriteHosts = readWriteHosts.ToRedisEndPoints();
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

        protected virtual void OnStart()
        {
            this.Start();
        }

        /// <summary>
        /// Returns a Read/Write client (The default) using the hosts defined in ReadWriteHosts
        /// </summary>
        /// <returns></returns>
        public IRedisClient GetClient()
        {
            lock (writeClients)
            {
                AssertValidReadWritePool();

                RedisClient inActiveClient;
                while ((inActiveClient = GetInActiveWriteClient()) == null)
                {
                    if (PoolTimeout.HasValue)
                    {
                        // wait for a connection, cry out if made to wait too long
                        if (!Monitor.Wait(writeClients, PoolTimeout.Value))
                            throw new TimeoutException(PoolTimeoutError);
                    }
                    else
                        Monitor.Wait(writeClients, RecheckPoolAfterMs);
                }

                WritePoolIndex++;
                inActiveClient.Active = true;

                if (this.ConnectTimeout != null)
                {
                    inActiveClient.ConnectTimeout = this.ConnectTimeout.Value;
                }

                if (this.SocketSendTimeout.HasValue)
                {
                    inActiveClient.SendTimeout = this.SocketSendTimeout.Value;
                }
                if (this.SocketReceiveTimeout.HasValue)
                {
                    inActiveClient.ReceiveTimeout = this.SocketReceiveTimeout.Value;
                }

                inActiveClient.NamespacePrefix = NamespacePrefix;

                //Reset database to default if changed
                if (inActiveClient.Db != Db)
                {
                    inActiveClient.ChangeDb(Db);
                }

                return inActiveClient;
            }
        }

        /// <summary>
        /// Called within a lock
        /// </summary>
        /// <returns></returns>
        private RedisClient GetInActiveWriteClient()
        {
            var desiredIndex = WritePoolIndex % writeClients.Length;
            //this will loop through all hosts in readClients once even though there are 2 for loops
            //both loops are used to try to get the prefered host according to the round robin algorithm
            for (int x = 0; x < ReadWriteHosts.Count; x++)
            {
                var nextHostIndex = (desiredIndex + x) % ReadWriteHosts.Count;
                var nextHost = ReadWriteHosts[nextHostIndex];
                for (var i = nextHostIndex; i < writeClients.Length; i += ReadWriteHosts.Count)
                {
                    if (writeClients[i] != null && !writeClients[i].Active && !writeClients[i].HadExceptions)
                        return writeClients[i];
                    else if (writeClients[i] == null || writeClients[i].HadExceptions)
                    {
                        if (writeClients[i] != null)
                            writeClients[i].DisposeConnection();
                        var client = RedisClientFactory.CreateRedisClient(nextHost.Host, nextHost.Port);

                        if (nextHost.RequiresAuth)
                            client.Password = nextHost.Password;

                        client.Id = RedisClientCounter++;
                        client.ClientManager = this;
                        client.NamespacePrefix = NamespacePrefix;
                        client.ConnectionFilter = ConnectionFilter;

                        writeClients[i] = client;

                        return client;
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Returns a ReadOnly client using the hosts defined in ReadOnlyHosts.
        /// </summary>
        /// <returns></returns>
        public virtual IRedisClient GetReadOnlyClient()
        {
            lock (readClients)
            {
                AssertValidReadOnlyPool();

                RedisClient inActiveClient;
                while ((inActiveClient = GetInActiveReadClient()) == null)
                {
                    if (PoolTimeout.HasValue)
                    {
                        // wait for a connection, cry out if made to wait too long
                        if (!Monitor.Wait(readClients, PoolTimeout.Value))
                            throw new TimeoutException(PoolTimeoutError);
                    }
                    else
                        Monitor.Wait(readClients, RecheckPoolAfterMs);
                }

                ReadPoolIndex++;
                inActiveClient.Active = true;

                if (this.ConnectTimeout != null)
                {
                    inActiveClient.ConnectTimeout = this.ConnectTimeout.Value;
                }

                if (this.SocketSendTimeout.HasValue)
                {
                    inActiveClient.SendTimeout = this.SocketSendTimeout.Value;
                }
                if (this.SocketReceiveTimeout.HasValue)
                {
                    inActiveClient.ReceiveTimeout = this.SocketReceiveTimeout.Value;
                }

                inActiveClient.NamespacePrefix = NamespacePrefix;

                //Reset database to default if changed
                if (inActiveClient.Db != Db)
                {
                    inActiveClient.ChangeDb(Db);
                }

                return inActiveClient;
            }
        }

        /// <summary>
        /// Called within a lock
        /// </summary>
        /// <returns></returns>
        private RedisClient GetInActiveReadClient()
        {
            var desiredIndex = ReadPoolIndex % readClients.Length;
            //this will loop through all hosts in readClients once even though there are 2 for loops
            //both loops are used to try to get the prefered host according to the round robin algorithm
            for (int x = 0; x < ReadOnlyHosts.Count; x++)
            {
                var nextHostIndex = (desiredIndex + x) % ReadOnlyHosts.Count;
                var nextHost = ReadOnlyHosts[nextHostIndex];
                for (var i = nextHostIndex; i < readClients.Length; i += ReadOnlyHosts.Count)
                {
                    if (readClients[i] != null && !readClients[i].Active && !readClients[i].HadExceptions)
                        return readClients[i];
                    else if (readClients[i] == null || readClients[i].HadExceptions)
                    {
                        if (readClients[i] != null)
                            readClients[i].DisposeConnection();
                        var client = RedisClientFactory.CreateRedisClient(nextHost.Host, nextHost.Port);

                        if (nextHost.RequiresAuth)
                            client.Password = nextHost.Password;

                        client.ClientManager = this;
                        client.ConnectionFilter = ConnectionFilter;

                        readClients[i] = client;

                        return client;
                    }
                }
            }
            return null;
        }

        public void DisposeClient(RedisNativeClient client)
        {
            lock (readClients)
            {
                for (var i = 0; i < readClients.Length; i++)
                {
                    var readClient = readClients[i];
                    if (client != readClient) continue;
                    client.Active = false;
                    Monitor.PulseAll(readClients);
                    return;
                }
            }

            lock (writeClients)
            {
                for (var i = 0; i < writeClients.Length; i++)
                {
                    var writeClient = writeClients[i];
                    if (client != writeClient) continue;
                    client.Active = false;
                    Monitor.PulseAll(writeClients);
                    return;
                }
            }

            //Client not found in any pool, pulse both pools.
            lock (readClients)
                Monitor.PulseAll(readClients);
            lock (writeClients)
                Monitor.PulseAll(writeClients);
        }

        /// <summary>
        /// Disposes the read only client.
        /// </summary>
        /// <param name="client">The client.</param>
        public void DisposeReadOnlyClient(RedisNativeClient client)
        {
            lock (readClients)
            {
                client.Active = false;
                Monitor.PulseAll(readClients);
            }
        }

        /// <summary>
        /// Disposes the write client.
        /// </summary>
        /// <param name="client">The client.</param>
        public void DisposeWriteClient(RedisNativeClient client)
        {
            lock (writeClients)
            {
                client.Active = false;
                Monitor.PulseAll(writeClients);
            }
        }

        public void Start()
        {
            if (writeClients.Length > 0 || readClients.Length > 0)
                throw new InvalidOperationException("Pool has already been started");

            writeClients = new RedisClient[Config.MaxWritePoolSize];
            WritePoolIndex = 0;

            readClients = new RedisClient[Config.MaxReadPoolSize];
            ReadPoolIndex = 0;
        }

        public Dictionary<string, string> GetStats()
        {
            var writeClientsPoolSize = writeClients.Length;
            var writeClientsCreated = 0;
            var writeClientsWithExceptions = 0;
            var writeClientsInUse = 0;
            var writeClientsConnected = 0;

            foreach (var client in writeClients)
            {
                if (client == null)
                {
                    writeClientsCreated++;
                    continue;
                }

                if (client.HadExceptions)
                    writeClientsWithExceptions++;
                if (client.Active)
                    writeClientsInUse++;
                if (client.IsSocketConnected())
                    writeClientsConnected++;
            }

            var readClientsPoolSize = readClients.Length;
            var readClientsCreated = 0;
            var readClientsWithExceptions = 0;
            var readClientsInUse = 0;
            var readClientsConnected = 0;

            foreach (var client in readClients)
            {
                if (client == null)
                {
                    readClientsCreated++;
                    continue;
                }

                if (client.HadExceptions)
                    readClientsWithExceptions++;
                if (client.Active)
                    readClientsInUse++;
                if (client.IsSocketConnected())
                    readClientsConnected++;
            }

            var ret = new Dictionary<string, string>
                {
                    {"writeClientsPoolSize", "" + writeClientsPoolSize},
                    {"writeClientsCreated", "" + writeClientsCreated},
                    {"writeClientsWithExceptions", "" + writeClientsWithExceptions},
                    {"writeClientsInUse", "" + writeClientsInUse},
                    {"writeClientsConnected", "" + writeClientsConnected},

                    {"readClientsPoolSize", "" + readClientsPoolSize},
                    {"readClientsCreated", "" + readClientsCreated},
                    {"readClientsWithExceptions", "" + readClientsWithExceptions},
                    {"readClientsInUse", "" + readClientsInUse},
                    {"readClientsConnected", "" + readClientsConnected},
                };

            return ret;
        }

        private void AssertValidReadWritePool()
        {
            if (writeClients.Length < 1)
                throw new InvalidOperationException("Need a minimum read-write pool size of 1, then call Start()");
        }

        private void AssertValidReadOnlyPool()
        {
            if (readClients.Length < 1)
                throw new InvalidOperationException("Need a minimum read pool size of 1, then call Start()");
        }

        public int[] GetClientPoolActiveStates()
        {
            var activeStates = new int[writeClients.Length];
            lock (writeClients)
            {
                for (int i = 0; i < writeClients.Length; i++)
                {
                    activeStates[i] = writeClients[i] == null
                        ? -1
                        : writeClients[i].Active ? 1 : 0;
                }
            }
            return activeStates;
        }

        public int[] GetReadOnlyClientPoolActiveStates()
        {
            var activeStates = new int[readClients.Length];
            lock (readClients)
            {
                for (int i = 0; i < readClients.Length; i++)
                {
                    activeStates[i] = readClients[i] == null
                        ? -1
                        : readClients[i].Active ? 1 : 0;
                }
            }
            return activeStates;
        }

        ~PooledRedisClientManager()
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
                for (var i = 0; i < writeClients.Length; i++)
                {
                    Dispose(writeClients[i]);
                }
                for (var i = 0; i < readClients.Length; i++)
                {
                    Dispose(readClients[i]);
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
    }
}
