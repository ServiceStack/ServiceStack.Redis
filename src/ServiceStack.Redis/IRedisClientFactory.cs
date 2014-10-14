//
// https://github.com/ServiceStack/ServiceStack.Redis
// ServiceStack.Redis: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Demis Bellot (demis.bellot@gmail.com)
//
// Copyright 2013 Service Stack LLC. All Rights Reserved.
//
// Licensed under the same terms of ServiceStack.
//

using System.Net;

namespace ServiceStack.Redis
{
	public interface IRedisClientFactory
	{
	    RedisClient CreateRedisClient(RedisEndpoint config);
	}
}