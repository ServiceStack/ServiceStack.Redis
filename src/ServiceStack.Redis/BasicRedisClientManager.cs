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
using ServiceStack.Common.Web;

namespace ServiceStack.Redis
{
	/// <summary>
	/// Provides thread-safe retrievel of redis clients since each client is a new one.
	/// Allows the configuration of different ReadWrite and ReadOnly hosts
	/// </summary>
	public partial class BasicRedisClientManager
		: IRedisClientsManager
	{
		private List<RedisEndPoint> ReadWriteHosts { get; set; }
		private List<RedisEndPoint> ReadOnlyHosts { get; set; }
        public int? ConnectTimeout { get; set; }

        /// <summary>
        /// Gets or sets object key prefix.
        /// </summary>
        public string NamespacePrefix { get; set; }
		private int readWriteHostsIndex;
		private int readOnlyHostsIndex;

		public IRedisClientFactory RedisClientFactory { get; set; }

		public int Db { get; private set; }

        public Action<IRedisNativeClient> ConnectionFilter { get; set; }

		public BasicRedisClientManager() : this(RedisNativeClient.DefaultHost) { }

		public BasicRedisClientManager(params string[] readWriteHosts)
			: this(readWriteHosts, readWriteHosts) {}

		public BasicRedisClientManager(int initialDb, params string[] readWriteHosts)
            : this(readWriteHosts, readWriteHosts, initialDb) {}

		/// <summary>
		/// Hosts can be an IP Address or Hostname in the format: host[:port]
		/// e.g. 127.0.0.1:6379
		/// default is: localhost:6379
		/// </summary>
		/// <param name="readWriteHosts">The write hosts.</param>
		/// <param name="readOnlyHosts">The read hosts.</param>
		public BasicRedisClientManager(
			IEnumerable<string> readWriteHosts,
			IEnumerable<string> readOnlyHosts)
			: this(readWriteHosts, readOnlyHosts, RedisNativeClient.DefaultDb)
		{
		}

		public BasicRedisClientManager(
			IEnumerable<string> readWriteHosts,
			IEnumerable<string> readOnlyHosts,
			int initalDb)
		{
			this.Db = initalDb;

			ReadWriteHosts = readWriteHosts.ToRedisEndPoints();
			ReadOnlyHosts = readOnlyHosts.ToRedisEndPoints();
			
			this.RedisClientFactory = Redis.RedisClientFactory.Instance;

			this.OnStart();
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
			var nextHost = ReadWriteHosts[readWriteHostsIndex++ % ReadWriteHosts.Count];
			var client = RedisClientFactory.CreateRedisClient(
				nextHost.Host, nextHost.Port);

            if (this.ConnectTimeout != null)
            {
                client.ConnectTimeout = this.ConnectTimeout.Value;
            }

			//Set database to userSpecified if different
			if (Db != RedisNativeClient.DefaultDb)
			{
				client.Db = Db;
			}

            if (nextHost.RequiresAuth)
                client.Password = nextHost.Password;

            client.NamespacePrefix = NamespacePrefix;
            client.ConnectionFilter = ConnectionFilter;

			return client;
		}

		/// <summary>
		/// Returns a ReadOnly client using the hosts defined in ReadOnlyHosts.
		/// </summary>
		/// <returns></returns>
		public virtual IRedisClient GetReadOnlyClient()
		{
			var nextHost = ReadOnlyHosts[readOnlyHostsIndex++ % ReadOnlyHosts.Count];
			var client = RedisClientFactory.CreateRedisClient(
				nextHost.Host, nextHost.Port);

            if (this.ConnectTimeout != null)
            {
                client.ConnectTimeout = this.ConnectTimeout.Value;
            }

			//Set database to userSpecified if different
			if (Db != RedisNativeClient.DefaultDb)
			{
				client.Db = Db;
			}

            if (nextHost.RequiresAuth)
                client.Password = nextHost.Password;

            client.NamespacePrefix = NamespacePrefix;
            client.ConnectionFilter = ConnectionFilter;

            return client;
		}

		public void SetAll<T>(IDictionary<string, T> values)
		{
			foreach (var entry in values)
			{
				Set(entry.Key, entry.Value);
			}
		}

		public void Start()
		{
			readWriteHostsIndex = 0;
			readOnlyHostsIndex = 0;
		}

		public void Dispose()
		{
		}
	}
}