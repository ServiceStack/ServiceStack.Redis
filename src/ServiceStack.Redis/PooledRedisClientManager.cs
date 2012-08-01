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
		: IRedisClientsManager
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(PooledRedisClientManager));

		private const string PoolTimeoutError =
				   "Redis Timeout expired. The timeout period elapsed prior to obtaining a connection from the pool. This may have occurred because all pooled connections were in use.";

		protected readonly int PoolSizeMultiplier = 10;
		public int? PoolTimeOut { get; set; }
        public int? ConnectTimeout { get; set; }
		public int? SocketSendTimeout { get; set; }
		public int? SocketReceiveTimeout { get; set; }

		private List<RedisEndPoint> ReadWriteHosts { get; set; }
		private List<RedisEndPoint> ReadOnlyHosts { get; set; }

		private RedisClient[] writeClients = new RedisClient[0];
		protected int WritePoolIndex;

		private RedisClient[] readClients = new RedisClient[0];
		protected int ReadPoolIndex;

		protected int RedisClientCounter = 0;

		protected RedisClientManagerConfig Config { get; set; }

		public IRedisClientFactory RedisClientFactory { get; set; }

		public int Db { get; private set; }

        public Action<IRedisNativeClient> ConnectionFilter { get; set; }

		public PooledRedisClientManager() : this(RedisNativeClient.DefaultHost) { }

		public PooledRedisClientManager(int poolSize, int poolTimeOutSeconds, params string[] readWriteHosts)
			: this(readWriteHosts, readWriteHosts, null, RedisNativeClient.DefaultDb, poolSize, poolTimeOutSeconds)
		{
		}

        public PooledRedisClientManager(int initialDb, params string[] readWriteHosts)
            : this(readWriteHosts, readWriteHosts, initialDb) {}

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
			int initalDb)
			: this(readWriteHosts, readOnlyHosts, null, initalDb, null, null)
		{
		}

		public PooledRedisClientManager(
			IEnumerable<string> readWriteHosts,
			IEnumerable<string> readOnlyHosts,
			RedisClientManagerConfig config,
			int initalDb,
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

			this.Config = config ?? new RedisClientManagerConfig {
				MaxWritePoolSize = ReadWriteHosts.Count * PoolSizeMultiplier,
				MaxReadPoolSize = ReadOnlyHosts.Count * PoolSizeMultiplier,
			};

			// if timeout provided, convert into milliseconds
			this.PoolTimeOut = poolTimeOutSeconds != null
				? poolTimeOutSeconds * 1000
				: null;


			if (this.Config.AutoStart)
			{
				this.OnStart();
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
					if (PoolTimeOut.HasValue)
					{
						// wait for a connection, cry out if made to wait too long
						if (!Monitor.Wait(writeClients, PoolTimeOut.Value))
							throw new TimeoutException(PoolTimeoutError);
					}
					else
						Monitor.Wait(writeClients);
				}

				WritePoolIndex++;
				inActiveClient.Active = true;

                if (this.ConnectTimeout != null)
                {
                    inActiveClient.ConnectTimeout = this.ConnectTimeout.Value;
                }

				if( this.SocketSendTimeout.HasValue )
				{
					inActiveClient.SendTimeout = this.SocketSendTimeout.Value;
				}
				if( this.SocketReceiveTimeout.HasValue )
				{
					inActiveClient.ReceiveTimeout = this.SocketReceiveTimeout.Value;
				}

				//Reset database to default if changed
				if (inActiveClient.Db != Db)
				{
					inActiveClient.Db = Db;
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
			for (var i=0; i < writeClients.Length; i++)
			{
				var nextIndex = (WritePoolIndex + i) % writeClients.Length;

				//Initialize if not exists
				var existingClient = writeClients[nextIndex];
				if (existingClient == null
					|| existingClient.HadExceptions)
				{
					if (existingClient != null)
					{
						existingClient.DisposeConnection();
					}

					var nextHost = ReadWriteHosts[nextIndex % ReadWriteHosts.Count];

					var client = RedisClientFactory.CreateRedisClient(
						nextHost.Host, nextHost.Port);

                    if (nextHost.RequiresAuth)
                        client.Password = nextHost.Password;

					client.Id = RedisClientCounter++;
					client.ClientManager = this;
				    client.ConnectionFilter = ConnectionFilter;

					writeClients[nextIndex] = client;

					return client;
				}

				//look for free one
				if (!writeClients[nextIndex].Active)
				{
					return writeClients[nextIndex];
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
					Monitor.Wait(readClients);
				}

				ReadPoolIndex++;
				inActiveClient.Active = true;

                if (this.ConnectTimeout != null)
                {
                    inActiveClient.ConnectTimeout = this.ConnectTimeout.Value;
                }

				if( this.SocketSendTimeout.HasValue )
				{
					inActiveClient.SendTimeout = this.SocketSendTimeout.Value;
				}
				if( this.SocketReceiveTimeout.HasValue )
				{
					inActiveClient.ReceiveTimeout = this.SocketReceiveTimeout.Value;
				}

				//Reset database to default if changed
				if (inActiveClient.Db != Db)
				{
					inActiveClient.Db = Db;
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
			for (var i=0; i < readClients.Length; i++)
			{
				var nextIndex = (ReadPoolIndex + i) % readClients.Length;

				//Initialize if not exists
				var existingClient = readClients[nextIndex];
				if (existingClient == null
					|| existingClient.HadExceptions)
				{
					if (existingClient != null)
					{
						existingClient.DisposeConnection();
					}

					var nextHost = ReadOnlyHosts[nextIndex % ReadOnlyHosts.Count];
					var client = RedisClientFactory.CreateRedisClient(
						nextHost.Host, nextHost.Port);

                    if (nextHost.RequiresAuth)
                        client.Password = nextHost.Password;

					client.ClientManager = this;
                    client.ConnectionFilter = ConnectionFilter;

					readClients[nextIndex] = client;

					return client;
				}

				//look for free one
				if (!readClients[nextIndex].Active)
				{
					return readClients[nextIndex];
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
			
			//Console.WriteLine("Couldn't find {0} client with Id: {1}, readclients: {2}, writeclients: {3}",
			//    client.IsDisposed ? "Disposed" : "Undisposed",
			//    client.Id,
			//    string.Join(", ", readClients.ToList().ConvertAll(x => x != null ? x.Id.ToString() : "").ToArray()),
			//    string.Join(", ", writeClients.ToList().ConvertAll(x => x != null ? x.Id.ToString() : "").ToArray()));

			if (client.IsDisposed) return;

			throw new NotSupportedException("Cannot add unknown client back to the pool");
		}

		/// <summary>
		/// Disposes the read only client.
		/// </summary>
		/// <param name="client">The client.</param>
		public void DisposeReadOnlyClient( RedisNativeClient client )
		{
			lock( readClients )
			{
				client.Active = false;
				Monitor.PulseAll( readClients );
			}
		}

		/// <summary>
		/// Disposes the write client.
		/// </summary>
		/// <param name="client">The client.</param>
		public void DisposeWriteClient( RedisNativeClient client )
		{
			lock( writeClients )
			{
				client.Active = false;
				Monitor.PulseAll( writeClients );
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
			if (disposing)
			{
				// get rid of managed resources
			}

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