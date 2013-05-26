using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceStack.Redis.Tests.Support
{
	internal static class RedisTestClientManagerFactory
	{
		internal static BasicRedisClientManager GetBasicRedisClientManagerInstance()
		{
			return new BasicRedisClientManager(new string[1]
			{
				TestConfig.SingleHostConnectionString
			});
		}
	}
}
