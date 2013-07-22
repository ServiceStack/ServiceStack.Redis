//
// https://github.com/mythz/ServiceStack.Redis
// ServiceStack.Redis: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Demis Bellot (demis.bellot@gmail.com)
//
// Copyright 2013 ServiceStack.
//
// Licensed under the same terms of Redis and ServiceStack: new BSD license.
//

using System.Net;

namespace ServiceStack.Redis
{
	/// <summary>
	/// Provide the default factory implementation for creating a RedisClient that 
	/// can be mocked and used by different 'Redis Client Managers' 
	/// </summary>
	public class RedisClientFactory : IRedisClientFactory
	{
		public static RedisClientFactory Instance = new RedisClientFactory();

		public RedisClient CreateRedisClient(string host, int port)
		{
			return new RedisClient(host, port);
		}

        public RedisClient CreateRedisClient(string host, int port, string password)
        {
            return new RedisClient(host, port, password);
        }
	}
}